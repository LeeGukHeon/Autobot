import json
import os
import shutil
import subprocess
import sys
import textwrap
from pathlib import Path

import pytest


REPO_ROOT = Path(__file__).resolve().parents[1]
DAILY_CC_SCRIPT = REPO_ROOT / "scripts" / "daily_champion_challenger_v4_for_server.ps1"


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def _powershell_exe() -> str:
    for name in ("powershell.exe", "pwsh"):
        resolved = shutil.which(name)
        if resolved:
            return resolved
    pytest.skip("PowerShell executable is required for this test")


def _git_exe() -> str:
    resolved = shutil.which("git")
    if not resolved:
        pytest.skip("git executable is required for this test")
    return resolved


def _make_fake_sudo(tmp_path: Path) -> Path:
    if os.name == "nt":
        wrapper_path = tmp_path / "sudo.cmd"
        wrapper_path.write_text(
            "@echo off\r\n%*\r\n",
            encoding="utf-8",
        )
    else:
        wrapper_path = tmp_path / "sudo"
        wrapper_path.write_text(
            "#!/bin/sh\n\"$@\"\n",
            encoding="utf-8",
        )
        wrapper_path.chmod(0o755)
    return wrapper_path


def _make_fake_systemctl(tmp_path: Path) -> Path:
    driver_path = tmp_path / "fake_systemctl_driver.py"
    driver_path.write_text(
        textwrap.dedent(
            """
            import json
            import os
            import sys
            from pathlib import Path

            KNOWN_UNITS = [
                "autobot-paper-v4.service",
                "autobot-paper-v4-challenger.service",
                "autobot-paper-v4-paired.service",
                "autobot-live-alpha.service",
                "autobot-live-alpha-candidate.service",
                "autobot-v4-challenger-spawn.service",
                "autobot-v4-challenger-promote.service",
                "autobot-v4-challenger-spawn.timer",
                "autobot-v4-challenger-promote.timer",
                "autobot-paper-v4-replay.service",
                "autobot-live-alpha-replay-shadow.service",
            ]

            def _split_env(name: str) -> list[str]:
                raw = os.environ.get(name, "")
                return [item.strip() for item in raw.split(",") if item.strip()]

            def _default_state() -> dict:
                unit_file_states = {
                    "autobot-paper-v4.service": "disabled",
                    "autobot-paper-v4-challenger.service": "disabled",
                    "autobot-paper-v4-paired.service": "enabled",
                    "autobot-live-alpha.service": "enabled",
                    "autobot-live-alpha-candidate.service": "enabled",
                    "autobot-v4-challenger-spawn.timer": "enabled",
                    "autobot-v4-challenger-promote.timer": "enabled",
                    "autobot-paper-v4-replay.service": "disabled",
                    "autobot-live-alpha-replay-shadow.service": "disabled",
                }
                for unit in _split_env("FAKE_DISABLED_UNIT_FILES"):
                    unit_file_states[unit] = "disabled"
                return {
                    "active_units": _split_env("FAKE_ACTIVE_UNITS"),
                    "failed_units": _split_env("FAKE_FAILED_UNITS"),
                    "unit_file_states": unit_file_states,
                }

            def _state_path() -> Path | None:
                raw = os.environ.get("FAKE_SYSTEMCTL_STATE", "").strip()
                if raw:
                    return Path(raw)
                return Path(__file__).with_name("fake_systemctl_state.json")

            def load_state() -> dict:
                state_path = _state_path()
                if state_path and state_path.exists():
                    return json.loads(state_path.read_text(encoding="utf-8"))
                state = _default_state()
                if state_path:
                    state_path.parent.mkdir(parents=True, exist_ok=True)
                    state_path.write_text(json.dumps(state), encoding="utf-8")
                return state

            def save_state(state: dict) -> None:
                state_path = _state_path()
                if not state_path:
                    return
                state_path.parent.mkdir(parents=True, exist_ok=True)
                state_path.write_text(json.dumps(state), encoding="utf-8")

            def log_command(argv: list[str]) -> None:
                log_path = os.environ.get("FAKE_SYSTEMCTL_LOG", "").strip()
                if log_path:
                    Path(log_path).write_text(
                        (Path(log_path).read_text(encoding="utf-8") if Path(log_path).exists() else "") + " ".join(argv) + "\\n",
                        encoding="utf-8",
                    )

            argv = sys.argv[1:]
            log_command(argv)
            state = load_state()
            active_units = set(state.get("active_units", []))
            failed_units = set(state.get("failed_units", []))
            unit_file_states = dict(state.get("unit_file_states", {}))

            def persist() -> None:
                save_state(
                    {
                        "active_units": sorted(active_units),
                        "failed_units": sorted(failed_units),
                        "unit_file_states": unit_file_states,
                    }
                )

            if not argv:
                sys.exit(0)

            cmd = argv[0]
            if cmd == "list-units":
                for unit in KNOWN_UNITS:
                    if unit in failed_units:
                        print(f"{unit} loaded failed failed Fake Unit")
                    elif unit in active_units:
                        sub = "waiting" if unit.endswith(".timer") else "running"
                        print(f"{unit} loaded active {sub} Fake Unit")
                    else:
                        print(f"{unit} loaded inactive dead Fake Unit")
                sys.exit(0)
            if cmd == "list-unit-files":
                for unit in KNOWN_UNITS:
                    if not (unit.endswith(".service") or unit.endswith(".timer")):
                        continue
                    print(f"{unit} {unit_file_states.get(unit, 'enabled')} enabled")
                sys.exit(0)
            if cmd == "is-active":
                target = argv[2] if len(argv) > 2 and argv[1] == "--quiet" else (argv[1] if len(argv) > 1 else "")
                sys.exit(0 if target in active_units else 1)
            if cmd == "enable":
                for unit in argv[1:]:
                    unit_file_states[unit] = "enabled"
                persist()
                sys.exit(0)
            if cmd in {"start", "restart"}:
                for unit in argv[1:]:
                    active_units.add(unit)
                    failed_units.discard(unit)
                persist()
                sys.exit(0)
            if cmd == "stop":
                for unit in argv[1:]:
                    active_units.discard(unit)
                    failed_units.discard(unit)
                persist()
                sys.exit(0)
            if cmd == "reset-failed":
                for unit in argv[1:]:
                    failed_units.discard(unit)
                persist()
                sys.exit(0)
            if cmd == "status":
                target = argv[1] if len(argv) > 1 else ""
                active = "active" if target in active_units else ("failed" if target in failed_units else "inactive")
                sub = "running" if target in active_units and target.endswith(".service") else ("waiting" if target in active_units else ("failed" if target in failed_units else "dead"))
                print(f"{target} {active} {sub}")
                sys.exit(0)
            sys.exit(0)
            """
        ).strip()
        + "\n",
        encoding="utf-8",
    )
    if os.name == "nt":
        wrapper_path = tmp_path / "systemctl.cmd"
        wrapper_path.write_text(
            f"@echo off\r\n\"{sys.executable}\" \"%~dp0fake_systemctl_driver.py\" %*\r\n",
            encoding="utf-8",
        )
    else:
        wrapper_path = tmp_path / "systemctl"
        wrapper_path.write_text(
            "#!/bin/sh\n"
            f'"{sys.executable}" "$(dirname "$0")/fake_systemctl_driver.py" "$@"\n',
            encoding="utf-8",
        )
        wrapper_path.chmod(0o755)
    return wrapper_path


def _make_fake_python(tmp_path: Path, compare_payload: dict | None = None) -> Path:
    payload_json = json.dumps(compare_payload or {"decision": {"promote": True, "decision": "promote_challenger"}})
    real_python = sys.executable.replace("\\", "\\\\")
    if os.name == "nt":
        wrapper_path = tmp_path / "fake_python.cmd"
        wrapper_path.write_text(
            "@echo off\r\n"
            "set args=%*\r\n"
            "echo %args% | findstr /C:\"autobot.ops.runtime_topology_report\" >nul\r\n"
            "if not errorlevel 1 (\r\n"
            f"  \"{real_python}\" -c \"import json,sys,pathlib; a=sys.argv[1:]; r=pathlib.Path(a[a.index('--project-root')+1]); p=r/'logs'/'runtime_topology'/'latest.json'; p.parent.mkdir(parents=True, exist_ok=True); p.write_text(json.dumps({{'summary': {{'topology_health_status': 'healthy', 'topology_health_reason_codes': [], 'champion_run_id': None, 'latest_candidate_run_id': None}}, 'topology_health': {{'status': 'healthy', 'reason_codes': []}}}}, ensure_ascii=False), encoding='utf-8'); print(p)\" %*\r\n"
            "  exit /b 0\r\n"
            ")\r\n"
            "echo %args% | findstr /C:\"autobot.ops.pointer_consistency_report\" >nul\r\n"
            "if not errorlevel 1 (\r\n"
            f"  \"{real_python}\" -c \"import json,sys,pathlib; a=sys.argv[1:]; r=pathlib.Path(a[a.index('--project-root')+1]); p=r/'logs'/'ops'/'pointer_consistency'/'latest.json'; p.parent.mkdir(parents=True, exist_ok=True); p.write_text(json.dumps({{'summary': {{'status': 'pass', 'violation_count': 0, 'warning_count': 0, 'reason_codes': []}}}}, ensure_ascii=False), encoding='utf-8'); print(p)\" %*\r\n"
            "  exit /b 0\r\n"
            ")\r\n"
            "echo %args% | findstr /C:\"autobot.common.paper_lane_evidence\" >nul\r\n"
            "if not errorlevel 1 (\r\n"
            f"  echo {payload_json}\r\n"
            "  exit /b 0\r\n"
            ")\r\n"
            "echo %args% | findstr /C:\"autobot.cli model promote\" >nul\r\n"
            "if not errorlevel 1 (\r\n"
            "  echo {}\r\n"
            "  exit /b 0\r\n"
            ")\r\n"
            "echo {}\r\n"
            "exit /b 0\r\n",
            encoding="utf-8",
        )
    else:
        wrapper_path = tmp_path / "fake_python"
        wrapper_path.write_text(
            "#!/bin/sh\n"
            "args=\"$*\"\n"
            "case \"$args\" in\n"
            f"  *autobot.ops.runtime_topology_report*) \"{sys.executable}\" -c 'import json,sys,pathlib; a=sys.argv[1:]; r=pathlib.Path(a[a.index(\"--project-root\")+1]); p=r/\"logs\"/\"runtime_topology\"/\"latest.json\"; p.parent.mkdir(parents=True, exist_ok=True); p.write_text(json.dumps({{\"summary\": {{\"topology_health_status\": \"healthy\", \"topology_health_reason_codes\": [], \"champion_run_id\": None, \"latest_candidate_run_id\": None}}, \"topology_health\": {{\"status\": \"healthy\", \"reason_codes\": []}}}}, ensure_ascii=False), encoding=\"utf-8\"); print(p)' \"$@\" ;;\n"
            f"  *autobot.ops.pointer_consistency_report*) \"{sys.executable}\" -c 'import json,sys,pathlib; a=sys.argv[1:]; r=pathlib.Path(a[a.index(\"--project-root\")+1]); p=r/\"logs\"/\"ops\"/\"pointer_consistency\"/\"latest.json\"; p.parent.mkdir(parents=True, exist_ok=True); p.write_text(json.dumps({{\"summary\": {{\"status\": \"pass\", \"violation_count\": 0, \"warning_count\": 0, \"reason_codes\": []}}}}, ensure_ascii=False), encoding=\"utf-8\"); print(p)' \"$@\" ;;\n"
            f"  *autobot.common.paper_lane_evidence*) printf '%s\\n' '{payload_json}' ;;\n"
            "  *'autobot.cli model promote'*) printf '{}\\n' ;;\n"
            "  *) printf '{}\\n' ;;\n"
            "esac\n",
            encoding="utf-8",
        )
        wrapper_path.chmod(0o755)
    return wrapper_path


def _make_fake_preflight_python(tmp_path: Path) -> Path:
    driver_path = tmp_path / "fake_preflight_python_driver.py"
    driver_path.write_text(
        textwrap.dedent(
            """
            import json
            import os
            import sys
            from pathlib import Path

            def _arg_value(name: str, default: str = "") -> str:
                if name not in sys.argv:
                    return default
                index = sys.argv.index(name)
                if index + 1 >= len(sys.argv):
                    return default
                return sys.argv[index + 1]

            def _load_state() -> dict:
                raw = os.environ.get("FAKE_SYSTEMCTL_STATE", "").strip()
                if raw and Path(raw).exists():
                    return json.loads(Path(raw).read_text(encoding="utf-8"))
                return {
                    "active_units": [item.strip() for item in os.environ.get("FAKE_ACTIVE_UNITS", "").split(",") if item.strip()],
                }

            args = sys.argv[1:]
            project_root = Path(_arg_value("--project-root")).resolve() if "--project-root" in args else Path.cwd()
            command_key = tuple(args[:2])

            if command_key == ("-m", "autobot.ops.runtime_topology_report"):
                report_path = project_root / "logs" / "runtime_topology" / "latest.json"
                report_path.parent.mkdir(parents=True, exist_ok=True)
                report_path.write_text(
                    json.dumps(
                        {
                            "summary": {
                                "topology_health_status": "healthy",
                                "topology_health_reason_codes": [],
                                "champion_run_id": "champion-run-001",
                                "latest_candidate_run_id": None,
                            },
                            "topology_health": {"status": "healthy", "reason_codes": []},
                        },
                        ensure_ascii=False,
                    ),
                    encoding="utf-8",
                )
                print(report_path)
                sys.exit(0)

            if command_key == ("-m", "autobot.ops.pointer_consistency_report"):
                state = _load_state()
                active_units = set(state.get("active_units", []))
                family_dir = project_root / "models" / "registry" / "train_v4_crypto_cs"
                latest_candidate_path = family_dir / "latest_candidate.json"
                latest_candidate = json.loads(latest_candidate_path.read_text(encoding="utf-8")) if latest_candidate_path.exists() else {}
                latest_candidate_run_id = str(latest_candidate.get("run_id", "")).strip() or None
                candidate_lane_active = "autobot-live-alpha-candidate.service" in active_units
                checks = []
                reason_codes = []
                status = "pass"
                if candidate_lane_active and not latest_candidate_run_id:
                    checks.append(
                        {
                            "code": "POINTER_CONSISTENCY_VIOLATION",
                            "status": "violation",
                            "message": "pointer consistency report summary is in violation state.",
                            "evidence": {
                                "status": "violation",
                                "reason_codes": ["CANDIDATE_UNITS_ACTIVE_WITHOUT_LATEST_CANDIDATE"],
                            },
                        }
                    )
                    reason_codes = ["CANDIDATE_UNITS_ACTIVE_WITHOUT_LATEST_CANDIDATE"]
                    status = "violation"
                report_path = project_root / "logs" / "ops" / "pointer_consistency" / "latest.json"
                report_path.parent.mkdir(parents=True, exist_ok=True)
                report_path.write_text(
                    json.dumps(
                        {
                            "checks": checks,
                            "summary": {
                                "status": status,
                                "violation_count": len(checks),
                                "warning_count": 0,
                                "reason_codes": reason_codes,
                            },
                        },
                        ensure_ascii=False,
                    ),
                    encoding="utf-8",
                )
                print(report_path)
                sys.exit(0)

            print("{}")
            sys.exit(0)
            """
        ).strip()
        + "\n",
        encoding="utf-8",
    )
    if os.name == "nt":
        wrapper_path = tmp_path / "fake_preflight_python.cmd"
        wrapper_path.write_text(
            f"@echo off\r\n\"{sys.executable}\" \"%~dp0fake_preflight_python_driver.py\" %*\r\n",
            encoding="utf-8",
        )
    else:
        wrapper_path = tmp_path / "fake_preflight_python"
        wrapper_path.write_text(
            "#!/bin/sh\n"
            f'"{sys.executable}" "$(dirname "$0")/fake_preflight_python_driver.py" "$@"\n',
            encoding="utf-8",
        )
        wrapper_path.chmod(0o755)
    return wrapper_path


def _make_fake_acceptance_script(
    tmp_path: Path,
    payload: dict,
    exit_code: int,
    *,
    emit_daily_micro_report: bool = False,
) -> Path:
    script_path = tmp_path / f"fake_acceptance_{exit_code}.ps1"
    payload_json = json.dumps(payload)
    prelude = ""
    if emit_daily_micro_report:
        prelude = textwrap.indent(
            textwrap.dedent(
                """
                $dailyReportPath = Join-Path $ProjectRoot "docs/reports/DAILY_MICRO_REPORT_2026-03-08.md"
                New-Item -ItemType Directory -Force -Path (Split-Path -Parent $dailyReportPath) | Out-Null
                "# fake daily report" | Set-Content -Path $dailyReportPath -Encoding UTF8
                Write-Host ("[daily-micro] report={0}" -f $dailyReportPath)
                """
            ).strip(),
            "            ",
        )
    script_path.write_text(
        textwrap.dedent(
            f"""
            param(
                [string]$ProjectRoot = "",
                [string]$PythonExe = "",
                [string]$BatchDate = "",
                [switch]$SkipPaperSoak,
                [switch]$SkipPromote,
                [switch]$SkipDailyPipeline,
                [switch]$SkipReportRefresh,
                [switch]$DryRun
            )

            $ErrorActionPreference = "Stop"
            $reportPath = Join-Path $ProjectRoot "logs/fake_acceptance/report.json"
            New-Item -ItemType Directory -Force -Path (Split-Path -Parent $reportPath) | Out-Null
            $payload = @'
            {payload_json}
            '@ | ConvertFrom-Json
            $candidateRunId = [string]($payload.steps.train.candidate_run_id)
            if (-not [string]::IsNullOrWhiteSpace($candidateRunId)) {{
                $runDir = Join-Path $ProjectRoot ("models/registry/train_v4_crypto_cs/" + $candidateRunId)
                New-Item -ItemType Directory -Force -Path $runDir | Out-Null
                @{{ "run_id" = $candidateRunId; "updated_at_utc" = "2026-03-24T00:00:00Z" }} |
                    ConvertTo-Json -Depth 8 | Set-Content -Path (Join-Path $runDir "latest.json") -Encoding UTF8
                @{{ "run_id" = $candidateRunId; "test_precision_top5" = 0.75 }} |
                    ConvertTo-Json -Depth 8 | Set-Content -Path (Join-Path $runDir "leaderboard_row.json") -Encoding UTF8
                @{{ "run_id" = $candidateRunId; "core_saved" = $true; "support_artifacts_written" = $true; "execution_acceptance_complete" = $true; "runtime_recommendations_complete" = $true; "governance_artifacts_complete" = $true; "acceptance_completed" = $true; "candidate_adoptable" = $true; "candidate_adopted" = $false; "promoted" = $false; "status" = "candidate"; "updated_at_utc" = "2026-03-24T00:00:00Z" }} |
                    ConvertTo-Json -Depth 8 | Set-Content -Path (Join-Path $runDir "artifact_status.json") -Encoding UTF8
            }}
            {prelude}
            $payload | ConvertTo-Json -Depth 20 | Set-Content -Path $reportPath -Encoding UTF8
            Write-Host ("[fake-accept] report={{0}}" -f $reportPath)
            exit {exit_code}
            """
        ).strip()
        + "\n",
        encoding="utf-8",
    )
    return script_path


def _make_fake_execution_policy_refresh_script(tmp_path: Path, rows_by_lookback: dict[int, int]) -> Path:
    script_path = tmp_path / "fake_execution_policy_refresh.ps1"
    payload_json = json.dumps({int(key): int(value) for key, value in rows_by_lookback.items()}, ensure_ascii=False)
    script_path.write_text(
        textwrap.dedent(
            f"""
            param(
                [string]$ProjectRoot = "",
                [string]$PythonExe = "",
                [int]$LookbackDays = 14,
                [switch]$DryRun
            )

            $outputDir = Join-Path $ProjectRoot "logs/fake_execution_policy"
            New-Item -ItemType Directory -Force -Path $outputDir | Out-Null
            $rowsLookup = @'
            {payload_json}
            '@ | ConvertFrom-Json
            $rowsTotal = 0
            $lookupKey = [string]([int]$LookbackDays)
            if ($rowsLookup.PSObject.Properties.Name -contains $lookupKey) {{
                $rowsTotal = [int]$rowsLookup.$lookupKey
            }}
            $reportPath = Join-Path $outputDir ("refresh_" + [int]$LookbackDays + ".json")
            [ordered]@{{
                policy = "fake_execution_policy_refresh"
                lookback_days = [int]$LookbackDays
                rows_total = [int]$rowsTotal
                execution_contract = [ordered]@{{
                    rows_total = [int]$rowsTotal
                }}
            }} | ConvertTo-Json -Depth 8 | Set-Content -Path $reportPath -Encoding UTF8
            Write-Host $reportPath
            exit 0
            """
        ).strip()
        + "\n",
        encoding="utf-8",
    )
    return script_path


def _seed_preflight_minimum(project_root: Path) -> None:
    family_dir = project_root / "models" / "registry" / "train_v4_crypto_cs"
    family_dir.mkdir(parents=True, exist_ok=True)
    (family_dir / "champion-run-001").mkdir(parents=True, exist_ok=True)
    (family_dir / "champion.json").write_text(json.dumps({"run_id": "champion-run-001"}), encoding="utf-8")
    (family_dir / "latest.json").write_text(json.dumps({"run_id": "champion-run-001"}), encoding="utf-8")
    (project_root / "models" / "registry" / "latest.json").write_text(
        json.dumps({"run_id": "champion-run-001", "model_family": "train_v4_crypto_cs"}),
        encoding="utf-8",
    )
    for state_db in (
        project_root / "data" / "state" / "live_candidate" / "live_state.db",
        project_root / "data" / "state" / "live_state.db",
    ):
        state_db.parent.mkdir(parents=True, exist_ok=True)
        state_db.write_text("", encoding="utf-8")


def _seed_latest_candidate_pointer(project_root: Path, run_id: str) -> None:
    family_dir = project_root / "models" / "registry" / "train_v4_crypto_cs"
    family_dir.mkdir(parents=True, exist_ok=True)
    (family_dir / run_id).mkdir(parents=True, exist_ok=True)
    (family_dir / "latest_candidate.json").write_text(json.dumps({"run_id": run_id}), encoding="utf-8")
    global_pointer = project_root / "models" / "registry" / "latest_candidate.json"
    global_pointer.parent.mkdir(parents=True, exist_ok=True)
    global_pointer.write_text(json.dumps({"run_id": run_id, "model_family": "train_v4_crypto_cs"}), encoding="utf-8")


def _make_fake_runtime_install_script(tmp_path: Path) -> Path:
    script_path = tmp_path / "fake_runtime_install.ps1"
    script_path.write_text(
        textwrap.dedent(
            """
            param(
                [string]$ProjectRoot = "",
                [string]$PythonExe = "",
                [string]$PaperUnitName = "",
                [string]$PaperModelFamilyOverride = "",
                [string]$PaperChampionModelFamilyOverride = "",
                [string]$PaperChallengerModelFamilyOverride = "",
                [string]$PaperModelRefPinned = "",
                [string]$PaperCliArgs = ""
            )

            $logPath = Join-Path $ProjectRoot "logs/fake_runtime_install/report.json"
            New-Item -ItemType Directory -Force -Path (Split-Path -Parent $logPath) | Out-Null
            @{
                paper_unit_name = $PaperUnitName
                paper_model_family_override = $PaperModelFamilyOverride
                paper_champion_model_family_override = $PaperChampionModelFamilyOverride
                paper_challenger_model_family_override = $PaperChallengerModelFamilyOverride
                paper_model_ref_pinned = $PaperModelRefPinned
                paper_cli_args = $PaperCliArgs
            } | ConvertTo-Json -Depth 4 | Set-Content -Path $logPath -Encoding UTF8
            Write-Host "[fake-runtime-install] ok"
            """
        ).strip()
        + "\n",
        encoding="utf-8",
    )
    return script_path


def _make_failing_runtime_install_script(tmp_path: Path) -> Path:
    script_path = tmp_path / "fake_runtime_install_fail.ps1"
    script_path.write_text(
        textwrap.dedent(
            """
            param(
                [string]$ProjectRoot = "",
                [string]$PythonExe = "",
                [string]$PaperUnitName = "",
                [string]$PaperModelFamilyOverride = "",
                [string]$PaperChampionModelFamilyOverride = "",
                [string]$PaperChallengerModelFamilyOverride = "",
                [string]$PaperModelRefPinned = "",
                [string]$PaperCliArgs = ""
            )

            Write-Error "runtime install failed"
            exit 1
            """
        ).strip()
        + "\n",
        encoding="utf-8",
    )
    return script_path


def _make_fake_paired_paper_script(
    tmp_path: Path,
    *,
    gate_pass: bool = True,
    reason: str = "PAIRED_PAPER_READY",
    matched_opportunities: int = 1,
) -> Path:
    script_path = tmp_path / f"fake_paired_paper_{'pass' if gate_pass else 'fail'}.ps1"
    payload_json = json.dumps(
        {
            "artifact_version": 1,
            "report_path": "",
            "gate": {
                "evaluated": True,
                "pair_ready": bool(gate_pass),
                "matched_opportunities": int(matched_opportunities),
                "min_matched_opportunities": 1,
                "pass": bool(gate_pass),
                "reason": str(reason),
            },
            "paired_report": {
                "clock_alignment": {
                    "pair_ready": bool(gate_pass),
                    "matched_opportunities": int(matched_opportunities),
                }
            },
            "promotion_decision": {
                "comparison_mode": "paired_paper_runtime_decision_v1",
                "paired_gate": {
                    "evaluated": True,
                    "pair_ready": bool(gate_pass),
                    "matched_opportunities": int(matched_opportunities),
                    "min_matched_opportunities": 1,
                    "pass": bool(gate_pass),
                    "reason": str(reason),
                },
                "decision": {
                    "promote": bool(gate_pass),
                    "decision": "promote_challenger" if gate_pass else "keep_champion",
                    "hard_failures": ([] if gate_pass else [str(reason)]),
                },
            },
        }
    )
    script_path.write_text(
        textwrap.dedent(
            f"""
            param(
                [string]$PythonExe = "",
                [string]$ProjectRoot = "",
                [int]$DurationSec = 0,
                [string]$Quote = "",
                [int]$TopN = 0,
                [string]$Tf = "",
                [string]$ChampionModelRef = "",
                [string]$ChallengerModelRef = "",
                [string]$ModelFamily = "",
                [string]$FeatureSet = "",
                [string]$Preset = "",
                [string]$PaperMicroProvider = "",
                [string]$PaperFeatureProvider = "",
                [int]$WarmupSec = 0,
                [int]$WarmupMinTradeEventsPerMarket = 0,
                [int]$MinMatchedOpportunities = 0,
                [double]$ReplayTimeScale = 0.0,
                [double]$ReplayMaxSleepSec = 0.0,
                [string]$OutDir = ""
            )

            $reportPath = Join-Path $ProjectRoot "logs/paired_paper/latest.json"
            New-Item -ItemType Directory -Force -Path (Split-Path -Parent $reportPath) | Out-Null
            $payload = @'
            {payload_json}
            '@ | ConvertFrom-Json
            $payload.report_path = $reportPath
            if (-not $payload.paired_report) {{
                $payload | Add-Member -NotePropertyName paired_report -NotePropertyValue ([PSCustomObject]@{{}}) -Force
            }}
            $payload.paired_report | Add-Member -NotePropertyName champion -NotePropertyValue ([PSCustomObject]@{{ paper_runtime_model_run_id = $ChampionModelRef }}) -Force
            $payload.paired_report | Add-Member -NotePropertyName challenger -NotePropertyValue ([PSCustomObject]@{{ paper_runtime_model_run_id = $ChallengerModelRef }}) -Force
            $payload | ConvertTo-Json -Depth 20 | Set-Content -Path $reportPath -Encoding UTF8
            Write-Host ("[paired-paper] report={{0}}" -f $reportPath)
            Write-Host ($payload | ConvertTo-Json -Depth 20)
            """
        ).strip()
        + "\n",
        encoding="utf-8",
    )
    return script_path


def _write_canary_confidence_sequence_artifact(
    project_root: Path,
    *,
    run_id: str,
    status: str,
    reason_codes: list[str] | None = None,
    sample_count: int = 12,
    unit_name: str = "autobot-live-alpha-candidate.service",
) -> Path:
    slug = "".join(ch.lower() if ch.isalnum() else "_" for ch in unit_name.strip().lower()).strip("_")
    slug = "_".join(part for part in slug.split("_") if part)
    path = project_root / "logs" / "canary_confidence_sequence" / slug / "latest.json"
    _write_json(
        path,
        {
            "policy": "canary_confidence_sequence_v1",
            "run_id": run_id,
            "decision": {
                "status": status,
                "reason_codes": list(reason_codes or []),
            },
            "reward_stream": {
                "sample_count": sample_count,
                "risk_adjusted_return_lcb": 0.1 if status == "promote_eligible" else (-0.1 if status == "abort" else 0.0),
                "return_ucb": -0.1 if status == "abort" else 0.1,
            },
        },
    )
    return path


def _run_spawn_only(
    project_root: Path,
    acceptance_script: Path,
    *,
    dry_run: bool = True,
    extra_args: list[str] | None = None,
    active_units: list[str] | None = None,
    python_exe: str | None = None,
    extra_env: dict[str, str] | None = None,
) -> subprocess.CompletedProcess[str]:
    _seed_preflight_minimum(project_root)
    sudo_dir = acceptance_script.parent
    _make_fake_sudo(sudo_dir)
    _make_fake_systemctl(sudo_dir)
    resolved_python_exe = str(python_exe or _make_fake_python(sudo_dir))
    systemctl_state_path = sudo_dir / "fake_systemctl_state.json"
    args = [
        _powershell_exe(),
        "-NoProfile",
        "-ExecutionPolicy",
        "Bypass",
        "-File",
        str(DAILY_CC_SCRIPT),
        "-ProjectRoot",
        str(project_root),
        "-PythonExe",
        resolved_python_exe,
        "-AcceptanceScript",
        str(acceptance_script),
        "-Mode",
        "spawn_only",
    ]
    if extra_args:
        args.extend(extra_args)
    if dry_run:
        args.append("-DryRun")
    return subprocess.run(
        args,
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        env={
            **os.environ,
            "PATH": str(sudo_dir) + os.pathsep + os.environ.get("PATH", ""),
            "FAKE_ACTIVE_UNITS": ",".join(active_units or []),
            "FAKE_SYSTEMCTL_STATE": str(systemctl_state_path),
            **(extra_env or {}),
        },
    )


def test_spawn_only_treats_candidate_rejection_as_successful_no_challenger_day(tmp_path: Path) -> None:
    project_root = tmp_path / "project"
    project_root.mkdir()

    acceptance_script = _make_fake_acceptance_script(
        tmp_path,
        {
            "steps": {
                "train": {"candidate_run_id": "candidate-run-001"},
            },
            "gates": {
                "backtest": {"pass": False},
                "overall_pass": False,
            },
            "reasons": ["BACKTEST_ACCEPTANCE_FAILED"],
        },
        exit_code=2,
    )

    completed = _run_spawn_only(project_root, acceptance_script, dry_run=False)

    assert completed.returncode == 0
    assert "[daily-cc] mode=spawn_only" in completed.stdout
    assert "[daily-cc] challenger_candidate_run_id=candidate-run-001" in completed.stdout


def test_spawn_only_retries_execution_contract_with_longer_lookback_before_failing(tmp_path: Path) -> None:
    project_root = tmp_path / "project"
    project_root.mkdir()

    acceptance_script = _make_fake_acceptance_script(
        tmp_path,
        {
            "steps": {
                "train": {"candidate_run_id": "candidate-run-lookback"},
            },
            "gates": {
                "backtest": {"pass": False},
                "overall_pass": False,
            },
            "reasons": ["BACKTEST_ACCEPTANCE_FAILED"],
        },
        exit_code=2,
    )
    refresh_script = _make_fake_execution_policy_refresh_script(tmp_path, {14: 9, 30: 24})

    completed = _run_spawn_only(
        project_root,
        acceptance_script,
        dry_run=False,
        extra_args=[
            "-ExecutionPolicyRefreshScript",
            str(refresh_script),
            "-ExecutionContractMinRows",
            "20",
        ],
    )

    assert completed.returncode == 0, completed.stdout + "\n" + completed.stderr
    latest = json.loads((project_root / "logs" / "model_v4_challenger" / "latest.json").read_text(encoding="utf-8-sig"))
    refresh_step = latest["steps"]["refresh_execution_contract"]

    assert refresh_step["rows_total"] == 24
    assert refresh_step["lookback_days"] == 30
    assert refresh_step["attempts"] == [
        {
            "lookback_days": 14,
            "exit_code": 0,
            "rows_total": 9,
            "output_path": str(project_root / "logs" / "fake_execution_policy" / "refresh_14.json"),
        },
        {
            "lookback_days": 30,
            "exit_code": 0,
            "rows_total": 24,
            "output_path": str(project_root / "logs" / "fake_execution_policy" / "refresh_30.json"),
        },
    ]


def test_spawn_only_uses_trainer_evidence_failure_as_root_start_reason(tmp_path: Path) -> None:
    project_root = tmp_path / "project"
    project_root.mkdir()

    acceptance_script = _make_fake_acceptance_script(
        tmp_path,
        {
            "steps": {
                "train": {"candidate_run_id": "candidate-run-evidence"},
            },
            "gates": {
                "backtest": {"pass": False},
                "overall_pass": False,
            },
            "reasons": ["BACKTEST_ACCEPTANCE_FAILED", "TRAINER_EVIDENCE_REQUIRED_FAILED"],
            "notes": ["PAPER_SOAK_SKIPPED"],
        },
        exit_code=2,
    )

    completed = _run_spawn_only(project_root, acceptance_script, dry_run=False)

    assert completed.returncode == 0
    latest = json.loads((project_root / "logs" / "model_v4_challenger" / "latest.json").read_text(encoding="utf-8-sig"))
    start_step = latest["steps"]["start_challenger"]
    assert start_step["reason"] == "TRAINER_EVIDENCE_REQUIRED_FAILED"
    assert start_step["acceptance_notes"] == ["PAPER_SOAK_SKIPPED"]


def test_spawn_only_treats_duplicate_candidate_as_successful_no_challenger_day(tmp_path: Path) -> None:
    project_root = tmp_path / "project"
    project_root.mkdir()

    acceptance_script = _make_fake_acceptance_script(
        tmp_path,
        {
            "steps": {
                "train": {"candidate_run_id": "candidate-run-dup"},
            },
            "gates": {
                "backtest": {"pass": False},
                "overall_pass": False,
            },
            "reasons": ["DUPLICATE_CANDIDATE"],
        },
        exit_code=2,
    )

    completed = _run_spawn_only(project_root, acceptance_script)

    assert completed.returncode == 0
    assert "[daily-cc] mode=spawn_only" in completed.stdout
    assert "[daily-cc] challenger_candidate_run_id=candidate-run-dup" in completed.stdout


def test_spawn_only_prefers_final_json_report_over_daily_markdown_report(tmp_path: Path) -> None:
    project_root = tmp_path / "project"
    project_root.mkdir()

    acceptance_script = _make_fake_acceptance_script(
        tmp_path,
        {
            "steps": {
                "train": {"candidate_run_id": "candidate-run-002"},
            },
            "gates": {
                "backtest": {"pass": False},
                "overall_pass": False,
            },
            "reasons": ["BACKTEST_ACCEPTANCE_FAILED"],
        },
        exit_code=2,
        emit_daily_micro_report=True,
    )

    completed = _run_spawn_only(project_root, acceptance_script, dry_run=False)

    assert completed.returncode == 0, completed.stdout + "\n" + completed.stderr
    assert "[daily-cc] challenger_candidate_run_id=candidate-run-002" in completed.stdout


def test_spawn_only_still_fails_when_acceptance_reports_runtime_exception(tmp_path: Path) -> None:
    project_root = tmp_path / "project"
    project_root.mkdir()

    acceptance_script = _make_fake_acceptance_script(
        tmp_path,
        {
            "steps": {
                "exception": {"message": "boom"},
                "train": {"candidate_run_id": "candidate-run-001"},
            },
            "gates": {
                "backtest": {"pass": False},
                "overall_pass": False,
            },
            "reasons": ["UNHANDLED_EXCEPTION"],
        },
        exit_code=2,
    )

    completed = _run_spawn_only(project_root, acceptance_script)

    assert completed.returncode != 0
    assert "candidate acceptance failed unexpectedly" in completed.stderr or "candidate acceptance failed unexpectedly" in completed.stdout


def test_spawn_only_restores_active_challenger_on_fatal_acceptance_failure(tmp_path: Path) -> None:
    project_root = tmp_path / "project"
    project_root.mkdir()

    acceptance_script = _make_fake_acceptance_script(
        tmp_path,
        {
            "steps": {
                "exception": {"message": "boom"},
                "train": {"candidate_run_id": "candidate-run-rollback"},
            },
            "gates": {
                "backtest": {"pass": False},
                "overall_pass": False,
            },
            "reasons": ["UNHANDLED_EXCEPTION"],
        },
        exit_code=2,
    )

    completed = _run_spawn_only(
        project_root,
        acceptance_script,
        dry_run=False,
        active_units=["autobot-paper-v4-challenger.service"],
    )

    assert completed.returncode == 2
    latest = json.loads((project_root / "logs" / "model_v4_challenger" / "latest.json").read_text(encoding="utf-8-sig"))
    rollback = latest["steps"]["rollback"]
    assert rollback["attempted"] is True
    assert rollback["restored_units"] == ["autobot-paper-v4-challenger.service"]


def test_spawn_only_runtime_install_failure_rolls_back_without_stale_state(tmp_path: Path) -> None:
    project_root = tmp_path / "project"
    project_root.mkdir()

    acceptance_script = _make_fake_acceptance_script(
        tmp_path,
        {
            "steps": {
                "train": {"candidate_run_id": "candidate-run-install-fail"},
            },
            "gates": {
                "backtest": {"pass": True},
                "overall_pass": False,
            },
            "reasons": [],
        },
        exit_code=0,
    )
    runtime_install_script = _make_failing_runtime_install_script(tmp_path)

    completed = _run_spawn_only(
        project_root,
        acceptance_script,
        dry_run=False,
        active_units=["autobot-paper-v4-challenger.service"],
        extra_args=[
            "-RuntimeInstallScript",
            str(runtime_install_script),
        ],
    )

    assert completed.returncode == 2
    latest = json.loads((project_root / "logs" / "model_v4_challenger" / "latest.json").read_text(encoding="utf-8-sig"))
    rollback = latest["steps"]["rollback"]
    assert rollback["attempted"] is True
    assert rollback["restored_units"] == ["autobot-paper-v4-challenger.service"]
    assert not (project_root / "logs" / "model_v4_challenger" / "current_state.json").exists()


def test_spawn_only_accepts_comma_joined_promotion_target_units_from_installer(tmp_path: Path) -> None:
    project_root = tmp_path / "project"
    project_root.mkdir()

    acceptance_script = _make_fake_acceptance_script(
        tmp_path,
        {
            "steps": {
                "train": {"candidate_run_id": "candidate-run-array"},
            },
            "gates": {
                "backtest": {"pass": False},
                "overall_pass": False,
            },
            "reasons": ["DUPLICATE_CANDIDATE"],
        },
        exit_code=2,
    )

    completed = _run_spawn_only(
        project_root,
        acceptance_script,
        dry_run=False,
        extra_args=[
            "-PromotionTargetUnits",
            "autobot-live-alpha.service,autobot-live-alpha-candidate.service",
        ],
    )

    assert completed.returncode == 0
    latest = json.loads((project_root / "logs" / "model_v4_challenger" / "latest.json").read_text(encoding="utf-8-sig"))
    assert latest["promotion_target_units"] == [
        "autobot-live-alpha.service",
        "autobot-live-alpha-candidate.service",
    ]


def test_spawn_only_starts_bootstrap_candidate_as_non_promotable_challenger(tmp_path: Path) -> None:
    project_root = tmp_path / "project"
    project_root.mkdir()

    acceptance_script = _make_fake_acceptance_script(
        tmp_path,
        {
            "candidate": {
                "lane_mode": "bootstrap_latest_inclusive",
                "promotion_eligible": False,
                "split_policy_artifact_path": str(project_root / "models" / "registry" / "train_v4_crypto_cs" / "candidate-run-bootstrap" / "split_policy_decision.json"),
            },
            "split_policy": {
                "policy_id": "v4_split_policy_bootstrap_transition_v1",
                "lane_mode": "bootstrap_latest_inclusive",
                "promotion_eligible": False,
            },
            "steps": {
                "train": {"candidate_run_id": "candidate-run-bootstrap"},
            },
            "gates": {
                "backtest": {"pass": False},
                "overall_pass": False,
            },
            "reasons": ["BOOTSTRAP_ONLY_POLICY"],
        },
        exit_code=2,
    )
    runtime_install_script = _make_fake_runtime_install_script(tmp_path)

    completed = _run_spawn_only(
        project_root,
        acceptance_script,
        dry_run=False,
        extra_args=[
            "-RuntimeInstallScript",
            str(runtime_install_script),
        ],
    )

    assert completed.returncode == 0, completed.stdout + "\n" + completed.stderr
    latest = json.loads((project_root / "logs" / "model_v4_challenger" / "latest.json").read_text(encoding="utf-8-sig"))
    state = json.loads((project_root / "logs" / "model_v4_challenger" / "current_state.json").read_text(encoding="utf-8-sig"))

    assert latest["steps"]["train_candidate"]["bootstrap_only"] is True
    assert latest["steps"]["start_challenger"]["candidate_run_id"] == "candidate-run-bootstrap"
    assert latest["steps"]["start_paired_paper"]["unit_name"] == "autobot-paper-v4-paired.service"
    assert latest["steps"]["start_paired_paper"]["candidate_run_id"] == "candidate-run-bootstrap"
    assert state["candidate_run_id"] == "candidate-run-bootstrap"
    assert state["lane_mode"] == "bootstrap_latest_inclusive"
    assert state["promotion_eligible"] is False
    assert state["bootstrap_only"] is True


def test_spawn_only_restarts_configured_candidate_targets_when_active(tmp_path: Path) -> None:
    project_root = tmp_path / "project"
    project_root.mkdir()
    runtime_install_script = _make_fake_runtime_install_script(tmp_path)

    acceptance_script = _make_fake_acceptance_script(
        tmp_path,
        {
            "candidate": {
                "lane_mode": "promotion_strict",
                "promotion_eligible": True,
            },
            "split_policy": {
                "lane_mode": "promotion_strict",
                "promotion_eligible": True,
            },
            "steps": {
                "train": {"candidate_run_id": "candidate-run-live-canary"},
            },
            "gates": {
                "backtest": {"pass": True},
                "overall_pass": True,
            },
            "reasons": [],
        },
        exit_code=0,
    )

    completed = _run_spawn_only(
        project_root,
        acceptance_script,
        dry_run=False,
        extra_args=[
            "-RuntimeInstallScript",
            str(runtime_install_script),
            "-CandidateTargetUnits",
            "autobot-live-alpha-candidate.service",
        ],
        active_units=["autobot-live-alpha-candidate.service"],
    )

    assert completed.returncode == 0, completed.stdout + "\n" + completed.stderr
    latest = json.loads((project_root / "logs" / "model_v4_challenger" / "latest.json").read_text(encoding="utf-8-sig"))

    restart_step = latest["steps"]["restart_candidate_targets"]
    assert restart_step["attempted"] is True
    assert restart_step["candidate_run_id"] == "candidate-run-live-canary"
    assert restart_step["restarted_units"] == ["autobot-live-alpha-candidate.service"]
    assert restart_step["started_from_inactive_units"] == []
    assert restart_step["skipped_units"] == []


def test_spawn_only_starts_inactive_candidate_targets_when_adoption_succeeds(tmp_path: Path) -> None:
    project_root = tmp_path / "project"
    project_root.mkdir()
    runtime_install_script = _make_fake_runtime_install_script(tmp_path)
    systemctl_log = tmp_path / "systemctl.log"
    _make_fake_sudo(tmp_path)
    _make_fake_systemctl(tmp_path)

    acceptance_script = _make_fake_acceptance_script(
        tmp_path,
        {
            "candidate": {
                "lane_mode": "promotion_strict",
                "promotion_eligible": True,
            },
            "split_policy": {
                "lane_mode": "promotion_strict",
                "promotion_eligible": True,
            },
            "steps": {
                "train": {"candidate_run_id": "candidate-run-live-canary-cold"},
            },
            "gates": {
                "backtest": {"pass": True},
                "overall_pass": True,
            },
            "reasons": [],
        },
        exit_code=0,
    )

    completed = _run_spawn_only(
        project_root,
        acceptance_script,
        dry_run=False,
        extra_args=[
            "-RuntimeInstallScript",
            str(runtime_install_script),
            "-CandidateTargetUnits",
            "autobot-live-alpha-candidate.service",
        ],
        active_units=[],
    )

    assert completed.returncode == 0, completed.stdout + "\n" + completed.stderr
    latest = json.loads((project_root / "logs" / "model_v4_challenger" / "latest.json").read_text(encoding="utf-8-sig"))

    restart_step = latest["steps"]["restart_candidate_targets"]
    assert restart_step["attempted"] is True
    assert restart_step["candidate_run_id"] == "candidate-run-live-canary-cold"
    assert restart_step["restarted_units"] == ["autobot-live-alpha-candidate.service"]
    assert restart_step["started_from_inactive_units"] == ["autobot-live-alpha-candidate.service"]
    assert restart_step["skipped_units"] == []


def test_spawn_only_does_not_restart_candidate_targets_when_overall_pass_is_false(tmp_path: Path) -> None:
    project_root = tmp_path / "project"
    project_root.mkdir()
    runtime_install_script = _make_fake_runtime_install_script(tmp_path)

    acceptance_script = _make_fake_acceptance_script(
        tmp_path,
        {
            "candidate": {
                "lane_mode": "promotion_strict",
                "promotion_eligible": True,
            },
            "split_policy": {
                "lane_mode": "promotion_strict",
                "promotion_eligible": True,
            },
            "steps": {
                "train": {"candidate_run_id": "candidate-run-live-canary-rejected"},
            },
            "gates": {
                "backtest": {"pass": True},
                "overall_pass": False,
            },
            "reasons": ["RUNTIME_PARITY_BACKTEST_FAILED"],
        },
        exit_code=2,
    )

    completed = _run_spawn_only(
        project_root,
        acceptance_script,
        dry_run=False,
        extra_args=[
            "-RuntimeInstallScript",
            str(runtime_install_script),
            "-CandidateTargetUnits",
            "autobot-live-alpha-candidate.service",
        ],
        active_units=["autobot-live-alpha-candidate.service"],
    )

    assert completed.returncode == 0, completed.stdout + "\n" + completed.stderr
    latest = json.loads((project_root / "logs" / "model_v4_challenger" / "latest.json").read_text(encoding="utf-8-sig"))

    restart_step = latest["steps"]["restart_candidate_targets"]
    assert restart_step["attempted"] is False
    assert restart_step["candidate_run_id"] == "candidate-run-live-canary-rejected"
    assert restart_step["reason"] == "OVERALL_PASS_REQUIRED"


def test_spawn_only_surfaces_execution_policy_veto_failure_reason(tmp_path: Path) -> None:
    project_root = tmp_path / "project"
    project_root.mkdir()
    runtime_install_script = _make_fake_runtime_install_script(tmp_path)

    acceptance_script = _make_fake_acceptance_script(
        tmp_path,
        {
            "candidate": {
                "lane_mode": "promotion_strict",
                "promotion_eligible": True,
            },
            "split_policy": {
                "lane_mode": "promotion_strict",
                "promotion_eligible": True,
            },
            "steps": {
                "train": {"candidate_run_id": "candidate-run-vetoed"},
            },
            "gates": {
                "backtest": {"pass": False},
                "overall_pass": False,
            },
            "reasons": ["BACKTEST_ACCEPTANCE_FAILED", "EXECUTION_POLICY_VETO_FAILURE"],
        },
        exit_code=2,
    )

    completed = _run_spawn_only(
        project_root,
        acceptance_script,
        dry_run=False,
        extra_args=[
            "-RuntimeInstallScript",
            str(runtime_install_script),
            "-CandidateTargetUnits",
            "autobot-live-alpha-candidate.service",
        ],
        active_units=["autobot-live-alpha-candidate.service"],
    )

    assert completed.returncode == 0, completed.stdout + "\n" + completed.stderr
    latest = json.loads((project_root / "logs" / "model_v4_challenger" / "latest.json").read_text(encoding="utf-8-sig"))

    start_step = latest["steps"]["start_challenger"]
    restart_step = latest["steps"]["restart_candidate_targets"]
    assert start_step["skipped"] is True
    assert start_step["reason"] == "EXECUTION_POLICY_VETO_FAILURE"
    assert restart_step["attempted"] is False
    assert restart_step["reason"] == "EXECUTION_POLICY_VETO_FAILURE"


def test_spawn_only_fails_fast_on_server_preflight_violation(tmp_path: Path) -> None:
    project_root = tmp_path / "project"
    project_root.mkdir()
    family_dir = project_root / "models" / "registry" / "train_v4_crypto_cs"
    family_dir.mkdir(parents=True, exist_ok=True)
    (family_dir / "champion.json").write_text(json.dumps({"run_id": "champion-run-000"}), encoding="utf-8")
    (family_dir / "champion-run-000").mkdir(parents=True, exist_ok=True)
    git_exe = _git_exe()
    subprocess.run([git_exe, "init"], cwd=project_root, check=True, capture_output=True, text=True)
    subprocess.run(
        [git_exe, "-c", "user.name=Test", "-c", "user.email=test@example.com", "commit", "--allow-empty", "-m", "init"],
        cwd=project_root,
        check=True,
        capture_output=True,
        text=True,
    )
    (project_root / "dirty.txt").write_text("dirty\n", encoding="utf-8")
    acceptance_script = _make_fake_acceptance_script(
        tmp_path,
        {
            "steps": {
                "train": {"candidate_run_id": "candidate-run-ignored"},
            },
            "gates": {
                "backtest": {"pass": True},
                "overall_pass": True,
            },
            "reasons": [],
        },
        exit_code=0,
    )

    completed = _run_spawn_only(project_root, acceptance_script, dry_run=False)

    assert completed.returncode == 2, completed.stdout + "\n" + completed.stderr
    latest = json.loads((project_root / "logs" / "model_v4_challenger" / "latest.json").read_text(encoding="utf-8-sig"))
    assert latest["steps"]["preflight"]["exit_code"] == 2
    assert latest["steps"]["preflight"]["summary"]["status"] == "violation"
    assert not (project_root / "logs" / "fake_acceptance" / "report.json").exists()
    preflight_report = json.loads((project_root / "logs" / "ops" / "server_preflight" / "latest.json").read_text(encoding="utf-8-sig"))
    assert preflight_report["required_pointers"] == ["champion"]
    assert preflight_report["check_candidate_state_consistency"] is True


def test_spawn_only_can_auto_recover_recoverable_server_preflight_violations(tmp_path: Path) -> None:
    project_root = tmp_path / "project"
    project_root.mkdir()
    acceptance_script = _make_fake_acceptance_script(
        tmp_path,
        {
            "steps": {
                "train": {"candidate_run_id": "candidate-run-001"},
            },
            "gates": {
                "backtest": {"pass": False},
                "overall_pass": False,
            },
            "reasons": ["BACKTEST_ACCEPTANCE_FAILED"],
        },
        exit_code=2,
    )
    systemctl_log = tmp_path / "systemctl.log"

    completed = _run_spawn_only(
        project_root,
        acceptance_script,
        dry_run=False,
        active_units=["autobot-live-alpha-candidate.service"],
        python_exe=str(_make_fake_preflight_python(tmp_path)),
        extra_env={
            "FAKE_DISABLED_UNIT_FILES": "autobot-v4-challenger-spawn.timer,autobot-v4-challenger-promote.timer",
            "FAKE_FAILED_UNITS": "autobot-v4-challenger-promote.service",
            "FAKE_SYSTEMCTL_LOG": str(systemctl_log),
        },
    )

    assert completed.returncode == 0, completed.stdout + "\n" + completed.stderr
    latest = json.loads((project_root / "logs" / "model_v4_challenger" / "latest.json").read_text(encoding="utf-8-sig"))
    assert latest["steps"]["preflight"]["exit_code"] == 0
    assert latest["steps"]["preflight_recovery"]["attempted"] is True
    assert latest["steps"]["preflight_recovery"]["applied"] is True
    assert latest["steps"]["preflight_recovery"]["reason"] == "RECOVERY_APPLIED"
    assert latest["steps"]["preflight_retry"]["exit_code"] == 0
    assert (project_root / "logs" / "fake_acceptance" / "report.json").exists()

    systemctl_commands = systemctl_log.read_text(encoding="utf-8")
    assert "enable autobot-v4-challenger-spawn.timer" in systemctl_commands
    assert "enable autobot-v4-challenger-promote.timer" in systemctl_commands
    assert "restart autobot-v4-challenger-spawn.timer" in systemctl_commands
    assert "restart autobot-v4-challenger-promote.timer" in systemctl_commands
    assert "reset-failed autobot-v4-challenger-promote.service" in systemctl_commands
    assert "stop autobot-live-alpha-candidate.service" in systemctl_commands


def test_spawn_only_clears_stale_previous_challenger_state_before_training(tmp_path: Path) -> None:
    project_root = tmp_path / "project"
    project_root.mkdir()
    _seed_preflight_minimum(project_root)
    _seed_latest_candidate_pointer(project_root, "candidate-run-stale")
    sudo_dir = tmp_path
    _make_fake_sudo(sudo_dir)
    _make_fake_systemctl(sudo_dir)
    acceptance_script = _make_fake_acceptance_script(
        tmp_path,
        {
            "steps": {
                "train": {"candidate_run_id": "candidate-run-new"},
            },
            "gates": {
                "backtest": {"pass": False},
                "overall_pass": False,
            },
            "reasons": ["BACKTEST_ACCEPTANCE_FAILED"],
        },
        exit_code=2,
    )

    state_path = project_root / "logs" / "model_v4_challenger" / "current_state.json"
    state_path.parent.mkdir(parents=True, exist_ok=True)
    state_path.write_text(
        json.dumps(
            {
                "batch_date": "2026-03-28",
                "candidate_run_id": "candidate-run-stale",
                "champion_run_id_at_start": "champion-run-001",
                "started_ts_ms": 1,
                "lane_mode": "promotion_strict",
                "promotion_eligible": True,
            }
        ),
        encoding="utf-8",
    )

    completed = _run_spawn_only(
        project_root,
        acceptance_script,
        dry_run=False,
        extra_args=["-CandidateTargetUnits", "autobot-live-alpha-candidate.service"],
        active_units=["autobot-live-alpha-candidate.service", "autobot-paper-v4-paired.service"],
        python_exe=str(_make_fake_python(tmp_path)),
    )

    assert completed.returncode == 0, completed.stdout + "\n" + completed.stderr
    latest = json.loads((project_root / "logs" / "model_v4_challenger" / "latest.json").read_text(encoding="utf-8-sig"))
    clear_step = latest["steps"]["clear_stale_previous_state"]

    assert clear_step["attempted"] is True
    assert clear_step["reason"] == "STALE_PREVIOUS_CHALLENGER_STATE"
    assert clear_step["candidate_run_id"] == "candidate-run-stale"
    assert clear_step["stop_units"]["stopped_units"] == [
        "autobot-paper-v4-paired.service",
        "autobot-live-alpha-candidate.service",
    ]
    assert latest["steps"]["train_candidate"]["candidate_run_id"] == "candidate-run-new"
    assert not state_path.exists()
    assert not (project_root / "models" / "registry" / "train_v4_crypto_cs" / "latest_candidate.json").exists()
    assert not (project_root / "models" / "registry" / "latest_candidate.json").exists()


def test_promote_only_skips_previous_bootstrap_candidate(tmp_path: Path) -> None:
    project_root = tmp_path / "project"
    project_root.mkdir()
    _seed_preflight_minimum(project_root)
    _seed_latest_candidate_pointer(project_root, "candidate-run-bootstrap")
    sudo_dir = tmp_path
    _make_fake_sudo(sudo_dir)
    _make_fake_systemctl(sudo_dir)
    state_path = project_root / "logs" / "model_v4_challenger" / "current_state.json"
    state_path.parent.mkdir(parents=True, exist_ok=True)
    state_path.write_text(
        json.dumps(
            {
                "batch_date": "2026-03-08",
                "candidate_run_id": "candidate-run-bootstrap",
                "champion_run_id_at_start": "champion-run-001",
                "started_ts_ms": 1,
                "lane_mode": "bootstrap_latest_inclusive",
                "promotion_eligible": False,
                "bootstrap_only": True,
            }
        ),
        encoding="utf-8",
    )

    completed = subprocess.run(
        [
            _powershell_exe(),
            "-NoProfile",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
            str(DAILY_CC_SCRIPT),
            "-ProjectRoot",
            str(project_root),
            "-PythonExe",
            sys.executable,
            "-Mode",
            "promote_only",
        ],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        env={**os.environ, "PATH": str(sudo_dir) + os.pathsep + os.environ.get("PATH", "")},
        check=False,
    )

    assert completed.returncode == 0, completed.stdout + "\n" + completed.stderr
    latest = json.loads((project_root / "logs" / "model_v4_challenger" / "latest.json").read_text(encoding="utf-8-sig"))

    assert latest["steps"]["promote_previous_challenger"]["reason"] == "BOOTSTRAP_ONLY_POLICY"
    assert latest["steps"]["promote_previous_challenger"]["promotion_eligible"] is False
    assert not state_path.exists()


def test_promote_only_starts_allowed_inactive_live_target_units(tmp_path: Path) -> None:
    project_root = tmp_path / "project"
    project_root.mkdir()
    _seed_preflight_minimum(project_root)
    _seed_latest_candidate_pointer(project_root, "candidate-run-promote")
    sudo_dir = tmp_path
    _make_fake_sudo(sudo_dir)
    _make_fake_systemctl(sudo_dir)
    fake_python = _make_fake_python(tmp_path)
    fake_paired = _make_fake_paired_paper_script(tmp_path)
    _write_canary_confidence_sequence_artifact(
        project_root,
        run_id="candidate-run-promote",
        status="promote_eligible",
    )

    state_path = project_root / "logs" / "model_v4_challenger" / "current_state.json"
    state_path.parent.mkdir(parents=True, exist_ok=True)
    state_path.write_text(
        json.dumps(
            {
                "batch_date": "2026-03-15",
                "candidate_run_id": "candidate-run-promote",
                "champion_run_id_at_start": "champion-run-001",
                "started_ts_ms": 1,
                "lane_mode": "promotion_strict",
                "promotion_eligible": True,
            }
        ),
        encoding="utf-8",
    )

    rollout_dir = project_root / "logs" / "live_rollout"
    rollout_dir.mkdir(parents=True, exist_ok=True)
    (rollout_dir / "latest.autobot_live_alpha_service.json").write_text(
        json.dumps(
            {
                "contract": {
                    "armed": True,
                    "mode": "live",
                    "target_unit": "autobot-live-alpha.service",
                }
            }
        ),
        encoding="utf-8",
    )

    systemctl_log = tmp_path / "systemctl.log"
    completed = subprocess.run(
        [
            _powershell_exe(),
            "-NoProfile",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
            str(DAILY_CC_SCRIPT),
            "-ProjectRoot",
            str(project_root),
            "-PythonExe",
            str(fake_python),
            "-PairedPaperScript",
            str(fake_paired),
            "-Mode",
            "promote_only",
            "-PromotionTargetUnits",
            "autobot-live-alpha.service",
            "-CandidateTargetUnits",
            "autobot-live-alpha-candidate.service",
        ],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        env={
            **os.environ,
            "PATH": str(sudo_dir) + os.pathsep + os.environ.get("PATH", ""),
            "FAKE_ACTIVE_UNITS": "",
            "FAKE_SYSTEMCTL_LOG": str(systemctl_log),
        },
        check=False,
    )

    assert completed.returncode == 0, completed.stdout + "\n" + completed.stderr
    latest = json.loads((project_root / "logs" / "model_v4_challenger" / "latest.json").read_text(encoding="utf-8-sig"))
    promote_step = latest["steps"]["promote_previous_challenger"]
    systemctl_calls = systemctl_log.read_text(encoding="utf-8")

    assert latest["steps"]["paired_paper_previous_challenger"]["report_path"].replace("\\", "/").endswith(
        "logs/paired_paper/latest.json"
    )
    assert promote_step["promoted"] is True
    assert promote_step["restarted_units"] == ["autobot-live-alpha.service"]
    assert promote_step["started_from_inactive_units"] == ["autobot-live-alpha.service"]
    assert promote_step["skipped_units"] == []
    assert "restart autobot-live-alpha.service" in systemctl_calls


def test_promote_only_aborts_and_clears_candidate_when_canary_aborts(tmp_path: Path) -> None:
    project_root = tmp_path / "project"
    project_root.mkdir()
    _seed_preflight_minimum(project_root)
    _seed_latest_candidate_pointer(project_root, "candidate-run-canary-abort")
    family_pointer = project_root / "models" / "registry" / "train_v4_crypto_cs" / "latest_candidate.json"
    global_pointer = project_root / "models" / "registry" / "latest_candidate.json"
    sudo_dir = tmp_path
    _make_fake_sudo(sudo_dir)
    _make_fake_systemctl(sudo_dir)
    fake_python = _make_fake_python(tmp_path)
    fake_paired = _make_fake_paired_paper_script(tmp_path)
    _write_canary_confidence_sequence_artifact(
        project_root,
        run_id="candidate-run-canary-abort",
        status="abort",
        reason_codes=["RISK_CONTROL_SEVERE_LOSS_RATE_CS_BREACH"],
    )

    state_path = project_root / "logs" / "model_v4_challenger" / "current_state.json"
    state_path.parent.mkdir(parents=True, exist_ok=True)
    state_path.write_text(
        json.dumps(
            {
                "batch_date": "2026-03-15",
                "candidate_run_id": "candidate-run-canary-abort",
                "champion_run_id_at_start": "champion-run-001",
                "started_ts_ms": 1,
                "lane_mode": "promotion_strict",
                "promotion_eligible": True,
            }
        ),
        encoding="utf-8",
    )

    completed = subprocess.run(
        [
            _powershell_exe(),
            "-NoProfile",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
            str(DAILY_CC_SCRIPT),
            "-ProjectRoot",
            str(project_root),
            "-PythonExe",
            str(fake_python),
            "-PairedPaperScript",
            str(fake_paired),
            "-Mode",
            "promote_only",
            "-CandidateTargetUnits",
            "autobot-live-alpha-candidate.service",
        ],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        env={
            **os.environ,
            "PATH": str(sudo_dir) + os.pathsep + os.environ.get("PATH", ""),
            "FAKE_ACTIVE_UNITS": "autobot-live-alpha-candidate.service",
        },
        check=False,
    )

    assert completed.returncode == 0, completed.stdout + "\n" + completed.stderr
    latest = json.loads((project_root / "logs" / "model_v4_challenger" / "latest.json").read_text(encoding="utf-8-sig"))

    assert latest["steps"]["promotion_state_machine"]["state"] == "abort"
    assert latest["steps"]["promote_previous_challenger"]["promoted"] is False
    assert latest["steps"]["promote_previous_challenger"]["reason"] == "RISK_CONTROL_SEVERE_LOSS_RATE_CS_BREACH"
    assert latest["steps"]["clear_latest_candidate"]["attempted"] is True
    assert not family_pointer.exists()
    assert not global_pointer.exists()
    assert not state_path.exists()


def test_promote_only_clears_latest_candidate_pointers_and_stops_candidate_targets(tmp_path: Path) -> None:
    project_root = tmp_path / "project"
    project_root.mkdir()
    _seed_preflight_minimum(project_root)
    _seed_latest_candidate_pointer(project_root, "candidate-run-promote-clear")
    family_pointer = project_root / "models" / "registry" / "train_v4_crypto_cs" / "latest_candidate.json"
    global_pointer = project_root / "models" / "registry" / "latest_candidate.json"
    sudo_dir = tmp_path
    _make_fake_sudo(sudo_dir)
    _make_fake_systemctl(sudo_dir)
    fake_python = _make_fake_python(tmp_path)
    fake_paired = _make_fake_paired_paper_script(tmp_path)
    fake_paired = _make_fake_paired_paper_script(tmp_path)
    fake_paired = _make_fake_paired_paper_script(tmp_path)
    _write_canary_confidence_sequence_artifact(
        project_root,
        run_id="candidate-run-promote-clear",
        status="promote_eligible",
    )

    state_path = project_root / "logs" / "model_v4_challenger" / "current_state.json"
    state_path.parent.mkdir(parents=True, exist_ok=True)
    state_path.write_text(
        json.dumps(
            {
                "batch_date": "2026-03-15",
                "candidate_run_id": "candidate-run-promote-clear",
                "champion_run_id_at_start": "champion-run-001",
                "started_ts_ms": 1,
                "lane_mode": "promotion_strict",
                "promotion_eligible": True,
            }
        ),
        encoding="utf-8",
    )

    systemctl_log = tmp_path / "systemctl.log"
    completed = subprocess.run(
        [
            _powershell_exe(),
            "-NoProfile",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
            str(DAILY_CC_SCRIPT),
            "-ProjectRoot",
            str(project_root),
            "-PythonExe",
            str(fake_python),
            "-PairedPaperScript",
            str(fake_paired),
            "-Mode",
            "promote_only",
            "-CandidateTargetUnits",
            "autobot-live-alpha-candidate.service",
        ],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        env={
            **os.environ,
            "PATH": str(sudo_dir) + os.pathsep + os.environ.get("PATH", ""),
            "FAKE_ACTIVE_UNITS": "autobot-live-alpha-candidate.service",
            "FAKE_SYSTEMCTL_LOG": str(systemctl_log),
        },
        check=False,
    )

    assert completed.returncode == 0, completed.stdout + "\n" + completed.stderr
    latest = json.loads((project_root / "logs" / "model_v4_challenger" / "latest.json").read_text(encoding="utf-8-sig"))
    systemctl_calls = systemctl_log.read_text(encoding="utf-8")

    assert latest["steps"]["promote_previous_challenger"]["promoted"] is True
    assert latest["steps"]["stop_candidate_targets_after_promote"]["stopped_units"] == [
        "autobot-live-alpha-candidate.service"
    ]
    assert latest["steps"]["clear_latest_candidate"]["removed_paths"] == [
        str(family_pointer),
        str(global_pointer),
    ]
    assert not family_pointer.exists()
    assert not global_pointer.exists()
    assert not state_path.exists()
    assert "stop autobot-live-alpha-candidate.service" in systemctl_calls


def test_promote_only_holds_when_paired_paper_gate_fails(tmp_path: Path) -> None:
    project_root = tmp_path / "project"
    project_root.mkdir()
    _seed_preflight_minimum(project_root)
    _seed_latest_candidate_pointer(project_root, "candidate-run-paired-fail")
    sudo_dir = tmp_path
    _make_fake_sudo(sudo_dir)
    _make_fake_systemctl(sudo_dir)
    fake_python = _make_fake_python(tmp_path)
    fake_paired = _make_fake_paired_paper_script(
        tmp_path,
        gate_pass=False,
        reason="PAIRED_PAPER_NOT_READY",
        matched_opportunities=0,
    )

    state_path = project_root / "logs" / "model_v4_challenger" / "current_state.json"
    family_pointer = project_root / "models" / "registry" / "train_v4_crypto_cs" / "latest_candidate.json"
    global_pointer = project_root / "models" / "registry" / "latest_candidate.json"
    state_path.parent.mkdir(parents=True, exist_ok=True)
    state_path.write_text(
        json.dumps(
            {
                "batch_date": "2026-03-15",
                "candidate_run_id": "candidate-run-paired-fail",
                "champion_run_id_at_start": "champion-run-001",
                "started_ts_ms": 1,
                "lane_mode": "promotion_strict",
                "promotion_eligible": True,
            }
        ),
        encoding="utf-8",
    )

    completed = subprocess.run(
        [
            _powershell_exe(),
            "-NoProfile",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
            str(DAILY_CC_SCRIPT),
            "-ProjectRoot",
            str(project_root),
            "-PythonExe",
            str(fake_python),
            "-PairedPaperScript",
            str(fake_paired),
            "-Mode",
            "promote_only",
        ],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        env={**os.environ, "PATH": str(sudo_dir) + os.pathsep + os.environ.get("PATH", "")},
        check=False,
    )

    assert completed.returncode == 0, completed.stdout + "\n" + completed.stderr
    latest = json.loads((project_root / "logs" / "model_v4_challenger" / "latest.json").read_text(encoding="utf-8-sig"))

    assert latest["steps"]["paired_paper_previous_challenger"]["gate"]["pass"] is False
    assert latest["steps"]["promotion_state_machine"]["state"] == "continue"
    assert latest["steps"]["promote_previous_challenger"]["promoted"] is False
    assert latest["steps"]["promote_previous_challenger"]["state_machine"] == "continue"
    assert latest["steps"]["clear_latest_candidate"]["attempted"] is False
    assert family_pointer.exists()
    assert global_pointer.exists()
    assert state_path.exists()


def test_spawn_then_promote_only_preserves_end_to_end_candidate_state_machine(tmp_path: Path) -> None:
    project_root = tmp_path / "project"
    project_root.mkdir()
    runtime_install_script = _make_fake_runtime_install_script(tmp_path)
    acceptance_script = _make_fake_acceptance_script(
        tmp_path,
        {
            "candidate": {
                "lane_mode": "promotion_strict",
                "promotion_eligible": True,
            },
            "split_policy": {
                "lane_mode": "promotion_strict",
                "promotion_eligible": True,
                "policy_id": "v4_split_policy_forward_validation_lcb_v1",
            },
            "steps": {
                "train": {"candidate_run_id": "candidate-run-e2e"},
            },
            "gates": {
                "backtest": {"pass": True},
                "overall_pass": True,
            },
            "reasons": [],
        },
        exit_code=0,
    )
    _write_json(
        project_root / "models" / "registry" / "train_v4_crypto_cs" / "champion.json",
        {"run_id": "champion-run-e2e"},
    )
    _write_json(
        project_root / "models" / "registry" / "train_v4_crypto_cs" / "latest.json",
        {"run_id": "champion-run-e2e"},
    )

    spawn_completed = _run_spawn_only(
        project_root,
        acceptance_script,
        dry_run=False,
        extra_args=[
            "-RuntimeInstallScript",
            str(runtime_install_script),
            "-CandidateTargetUnits",
            "autobot-live-alpha-candidate.service",
        ],
    )

    assert spawn_completed.returncode == 0, spawn_completed.stdout + "\n" + spawn_completed.stderr
    family_pointer = project_root / "models" / "registry" / "train_v4_crypto_cs" / "latest_candidate.json"
    global_pointer = project_root / "models" / "registry" / "latest_candidate.json"
    state_path = project_root / "logs" / "model_v4_challenger" / "current_state.json"
    spawn_latest = json.loads((project_root / "logs" / "model_v4_challenger" / "latest.json").read_text(encoding="utf-8-sig"))
    assert family_pointer.exists()
    assert global_pointer.exists()
    assert state_path.exists()
    assert spawn_latest["steps"]["start_paired_paper"]["unit_name"] == "autobot-paper-v4-paired.service"
    assert spawn_latest["steps"]["start_paired_paper"]["candidate_run_id"] == "candidate-run-e2e"

    sudo_dir = tmp_path
    _make_fake_sudo(sudo_dir)
    _make_fake_systemctl(sudo_dir)
    fake_python = _make_fake_python(tmp_path)
    fake_paired = _make_fake_paired_paper_script(tmp_path)
    _write_canary_confidence_sequence_artifact(
        project_root,
        run_id="candidate-run-e2e",
        status="promote_eligible",
    )
    systemctl_log = tmp_path / "systemctl.log"
    promote_completed = subprocess.run(
        [
            _powershell_exe(),
            "-NoProfile",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
            str(DAILY_CC_SCRIPT),
            "-ProjectRoot",
            str(project_root),
            "-PythonExe",
            str(fake_python),
            "-PairedPaperScript",
            str(fake_paired),
            "-Mode",
            "promote_only",
            "-CandidateTargetUnits",
            "autobot-live-alpha-candidate.service",
        ],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        env={
            **os.environ,
            "PATH": str(sudo_dir) + os.pathsep + os.environ.get("PATH", ""),
            "FAKE_ACTIVE_UNITS": "autobot-live-alpha-candidate.service",
            "FAKE_SYSTEMCTL_LOG": str(systemctl_log),
        },
        check=False,
    )

    assert promote_completed.returncode == 0, promote_completed.stdout + "\n" + promote_completed.stderr
    latest = json.loads((project_root / "logs" / "model_v4_challenger" / "latest.json").read_text(encoding="utf-8-sig"))
    cutover = json.loads(
        (project_root / "logs" / "model_v4_challenger" / "latest_promote_cutover.json").read_text(
            encoding="utf-8-sig"
        )
    )
    systemctl_calls = systemctl_log.read_text(encoding="utf-8")

    assert latest["steps"]["promote_previous_challenger"]["promoted"] is True
    assert latest["steps"]["promote_previous_challenger"]["candidate_run_id"] == "candidate-run-e2e"
    assert latest["steps"]["clear_latest_candidate"]["removed_paths"] == [
        str(family_pointer),
        str(global_pointer),
    ]
    assert latest["steps"]["stop_candidate_targets_after_promote"]["stopped_units"] == [
        "autobot-live-alpha-candidate.service"
    ]
    assert cutover["new_champion_run_id"] == "candidate-run-e2e"
    assert not family_pointer.exists()
    assert not global_pointer.exists()
    assert not state_path.exists()
    assert "stop autobot-live-alpha-candidate.service" in systemctl_calls
