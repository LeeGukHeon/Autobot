$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

function Get-PowerShellExe {
    if ($script:IsWindowsPlatform) {
        return "powershell.exe"
    }
    $cmd = Get-Command pwsh -ErrorAction SilentlyContinue
    if ($null -ne $cmd -and -not [string]::IsNullOrWhiteSpace($cmd.Source)) {
        $resolved = [string]$cmd.Source
        if (-not $resolved.StartsWith("/snap/")) {
            return $resolved
        }
    }
    foreach ($candidatePath in @(
        "/usr/bin/pwsh",
        "/usr/local/bin/pwsh",
        "/opt/microsoft/powershell/7/pwsh"
    )) {
        if (Test-Path $candidatePath) {
            return $candidatePath
        }
    }
    if ($null -ne $cmd -and -not [string]::IsNullOrWhiteSpace($cmd.Source)) {
        return [string]$cmd.Source
    }
    return "pwsh"
}

function Resolve-RegistryPointerPath {
    param(
        [string]$RegistryRoot,
        [string]$Family,
        [string]$PointerName
    )
    return (Join-Path (Join-Path $RegistryRoot $Family) ($PointerName + ".json"))
}

function Invoke-BacktestAndLoadSummary {
    param(
        [string]$PythonPath,
        [string]$Root,
        [string]$ModelRef,
        [string]$StartDate,
        [string]$EndDate,
        [ValidateSet("acceptance", "runtime_parity")]
        [string]$Preset = "acceptance"
    )
    $args = @(
        "-m", "autobot.cli",
        "backtest", "alpha",
        "--preset", $Preset,
        "--model-ref", $ModelRef,
        "--model-family", $ModelFamily,
        "--feature-set", $FeatureSet,
        "--tf", $Tf,
        "--quote", $Quote,
        "--top-n", $BacktestTopN,
        "--start", $StartDate,
        "--end", $EndDate
    )
    if ($Preset -eq "acceptance") {
        $args += @(
            "--top-pct", $BacktestTopPct,
            "--min-prob", $BacktestMinProb,
            "--min-cands-per-ts", $BacktestMinCandidatesPerTs,
            "--exit-mode", "hold",
            "--hold-bars", $HoldBars
        )
    }
    $exec = Invoke-CommandCapture -Exe $PythonPath -ArgList $args
    $runDir = if ($DryRun) { "" } else { Resolve-RunDirFromText -TextValue ([string]$exec.Output) }
    if ((-not $DryRun) -and [string]::IsNullOrWhiteSpace($runDir)) {
        throw "backtest run completed but run_dir was not reported by CLI stdout"
    }
    if ((-not $DryRun) -and (-not (Test-Path $runDir))) {
        throw "backtest run_dir does not exist: $runDir"
    }
    $summaryPath = if ([string]::IsNullOrWhiteSpace($runDir)) { "" } else { Join-Path $runDir "summary.json" }
    if ((-not $DryRun) -and (-not [string]::IsNullOrWhiteSpace($summaryPath)) -and (-not (Test-Path $summaryPath))) {
        throw "backtest summary.json does not exist: $summaryPath"
    }
    $summary = if ([string]::IsNullOrWhiteSpace($summaryPath)) { @{} } else { Load-JsonOrEmpty -PathValue $summaryPath }
    return [PSCustomObject]@{
        Exec = $exec
        RunDir = $runDir
        SummaryPath = $summaryPath
        Summary = $summary
    }
}

function Invoke-RestartUnits {
    param([string[]]$UnitsToRestart)
    $results = @()
    foreach ($unit in $UnitsToRestart) {
        $trimmed = [string]$unit
        if ([string]::IsNullOrWhiteSpace($trimmed)) {
            continue
        }
        $trimmed = $trimmed.Trim()
        if ($script:IsWindowsPlatform) {
            $results += [ordered]@{
                unit = $trimmed
                attempted = $false
                active = $false
                reason = "WINDOWS_SYSTEMCTL_UNAVAILABLE"
            }
            continue
        }
        $restartExec = Invoke-CommandCapture -Exe "sudo" -ArgList @("systemctl", "restart", $trimmed) -AllowFailure
        $activeExec = Invoke-CommandCapture -Exe "systemctl" -ArgList @("is-active", $trimmed) -AllowFailure
        $results += [ordered]@{
            unit = $trimmed
            attempted = $true
            restart_exit_code = [int]$restartExec.ExitCode
            restart_command = $restartExec.Command
            restart_output_preview = (Get-OutputPreview -Text ([string]$restartExec.Output))
            active = ($activeExec.ExitCode -eq 0) -and (([string]$activeExec.Output).Trim() -eq "active")
            active_command = $activeExec.Command
            active_output_preview = (Get-OutputPreview -Text ([string]$activeExec.Output))
        }
    }
    return @($results)
}

function Get-UnitStates {
    param([string[]]$Units)
    $states = @()
    foreach ($unit in $Units) {
        $trimmed = [string]$unit
        if ([string]::IsNullOrWhiteSpace($trimmed)) {
            continue
        }
        $trimmed = $trimmed.Trim()
        if ($script:IsWindowsPlatform) {
            $states += [ordered]@{
                unit = $trimmed
                active = $false
                enabled = $false
                reason = "WINDOWS_SYSTEMCTL_UNAVAILABLE"
            }
            continue
        }
        $activeExec = Invoke-CommandCapture -Exe "systemctl" -ArgList @("is-active", $trimmed) -AllowFailure
        $enabledExec = Invoke-CommandCapture -Exe "systemctl" -ArgList @("is-enabled", $trimmed) -AllowFailure
        $states += [ordered]@{
            unit = $trimmed
            active = ($activeExec.ExitCode -eq 0) -and (([string]$activeExec.Output).Trim() -eq "active")
            enabled = ($enabledExec.ExitCode -eq 0) -and (([string]$enabledExec.Output).Trim() -eq "enabled")
            active_command = $activeExec.Command
            enabled_command = $enabledExec.Command
            active_output_preview = (Get-OutputPreview -Text ([string]$activeExec.Output))
            enabled_output_preview = (Get-OutputPreview -Text ([string]$enabledExec.Output))
        }
    }
    return @($states)
}
