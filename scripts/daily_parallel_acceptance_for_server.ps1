param(
    [string]$ProjectRoot = "",
    [string]$PythonExe = "",
    [string]$DailyPipelineScript = "",
    [string]$V3AcceptanceScript = "",
    [string]$V4AcceptanceScript = "",
    [string]$BatchDate = "",
    [string]$Quote = "KRW",
    [int]$TrainTopN = 50,
    [string]$Tf = "5m",
    [int]$V3TrainLookbackDays = 30,
    [int]$V4TrainLookbackDays = 30,
    [int]$LanePollIntervalSec = 30,
    [int]$LaneStallTimeoutSec = 21600,
    [switch]$SkipDailyPipeline,
    [switch]$SkipFeaturesBuild,
    [switch]$SkipV3,
    [switch]$SkipV4,
    [switch]$DryRun
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

. (Join-Path $PSScriptRoot "systemd_service_utils.ps1")

function Resolve-DefaultDailyPipelineScript {
    param([string]$Root)
    return (Join-Path $Root "scripts/daily_micro_pipeline_for_server.ps1")
}

function Resolve-DefaultV3AcceptanceScript {
    param([string]$Root)
    return (Join-Path $Root "scripts/v3_candidate_acceptance.ps1")
}

function Resolve-DefaultV4AcceptanceScript {
    param([string]$Root)
    return (Join-Path $Root "scripts/v4_scout_candidate_acceptance.ps1")
}

function Resolve-DateToken {
    param([string]$DateText, [string]$LabelForError)
    if ([string]::IsNullOrWhiteSpace($DateText)) {
        throw "$LabelForError is empty"
    }
    try {
        $parsed = [DateTime]::ParseExact(
            $DateText,
            "yyyy-MM-dd",
            [System.Globalization.CultureInfo]::InvariantCulture,
            [System.Globalization.DateTimeStyles]::None
        )
        return $parsed.ToString("yyyy-MM-dd")
    } catch {
        throw "$LabelForError must be yyyy-MM-dd (actual='$DateText')"
    }
}

function Get-OutputPreview {
    param([string]$Text, [int]$MaxLength = 400)
    if ([string]::IsNullOrWhiteSpace($Text)) {
        return ""
    }
    $preview = $Text.Trim() -replace "\r?\n", " | "
    if ($preview.Length -le $MaxLength) {
        return $preview
    }
    return $preview.Substring(0, $MaxLength)
}

function Get-PropValue {
    param(
        [Parameter(Mandatory = $false)]$ObjectValue,
        [Parameter(Mandatory = $true)][string]$Name,
        [Parameter(Mandatory = $false)]$DefaultValue = $null
    )
    if ($null -eq $ObjectValue) {
        return $DefaultValue
    }
    if ($ObjectValue -is [System.Collections.IDictionary]) {
        if ($ObjectValue.Contains($Name)) {
            return $ObjectValue[$Name]
        }
        return $DefaultValue
    }
    if ($ObjectValue.PSObject -and $ObjectValue.PSObject.Properties.Name -contains $Name) {
        return $ObjectValue.$Name
    }
    return $DefaultValue
}

function Invoke-CommandCapture {
    param(
        [string]$Exe,
        [string[]]$ArgList
    )
    $commandText = $Exe + " " + (($ArgList | ForEach-Object { Quote-ShellArg ([string]$_) }) -join " ")
    if ($DryRun) {
        Write-Host ("[daily-parallel][dry-run] {0}" -f $commandText)
        return [PSCustomObject]@{
            ExitCode = 0
            Output = "[dry-run] $commandText"
            Command = $commandText
        }
    }
    $output = & $Exe @ArgList 2>&1
    return [PSCustomObject]@{
        ExitCode = [int]$LASTEXITCODE
        Output = ($output -join "`n")
        Command = $commandText
    }
}

function Start-AcceptanceProcess {
    param(
        [string]$PwshExe,
        [string[]]$ArgList,
        [string]$LogPrefix,
        [string]$LogDir,
        [string]$WorkingDirectory
    )
    $stamp = Get-Date -Format "yyyyMMdd-HHmmss"
    $stdoutPath = Join-Path $LogDir ($LogPrefix + "_" + $stamp + ".stdout.log")
    $stderrPath = Join-Path $LogDir ($LogPrefix + "_" + $stamp + ".stderr.log")
    $process = Start-Process -FilePath $PwshExe -ArgumentList $ArgList -WorkingDirectory $WorkingDirectory -RedirectStandardOutput $stdoutPath -RedirectStandardError $stderrPath -PassThru
    return [PSCustomObject]@{
        Process = $process
        StdoutPath = $stdoutPath
        StderrPath = $stderrPath
        StartedAtUtc = [DateTime]::UtcNow
        Command = $PwshExe + " " + (($ArgList | ForEach-Object { Quote-ShellArg ([string]$_) }) -join " ")
    }
}

function Get-FileProgressState {
    param([string]$PathValue)
    if ([string]::IsNullOrWhiteSpace($PathValue) -or (-not (Test-Path $PathValue))) {
        return [PSCustomObject]@{
            Exists = $false
            Length = 0
            LastWriteTicks = 0
        }
    }
    $item = Get-Item -Path $PathValue -ErrorAction SilentlyContinue
    if ($null -eq $item) {
        return [PSCustomObject]@{
            Exists = $false
            Length = 0
            LastWriteTicks = 0
        }
    }
    return [PSCustomObject]@{
        Exists = $true
        Length = [int64]$item.Length
        LastWriteTicks = [int64]$item.LastWriteTimeUtc.Ticks
    }
}

function Get-ProgressFingerprint {
    param([string[]]$Paths)
    $tokens = @()
    foreach ($pathValue in @($Paths)) {
        $state = Get-FileProgressState -PathValue $pathValue
        $tokens += ("{0}|{1}|{2}|{3}" -f ([string]$pathValue), [int]$state.Exists, [int64]$state.Length, [int64]$state.LastWriteTicks)
    }
    return ($tokens -join ";")
}

function Stop-ProcessTreeBestEffort {
    param([int]$ProcessId)
    if ($ProcessId -le 0) {
        return
    }
    if ([System.IO.Path]::DirectorySeparatorChar -eq '\') {
        $taskkillPath = Join-Path $env:SystemRoot "System32\taskkill.exe"
        if (Test-Path $taskkillPath) {
            & $taskkillPath /PID $ProcessId /T /F *> $null
            return
        }
        Stop-Process -Id $ProcessId -Force -ErrorAction SilentlyContinue
        return
    }
    $killScript = "pkill -TERM -P $ProcessId >/dev/null 2>&1 || true; kill -TERM $ProcessId >/dev/null 2>&1 || true; sleep 2; pkill -KILL -P $ProcessId >/dev/null 2>&1 || true; kill -KILL $ProcessId >/dev/null 2>&1 || true"
    & /bin/bash -lc $killScript *> $null
}

function Wait-AcceptanceProcessWithWatchdog {
    param(
        [string]$LaneName,
        [Parameter(Mandatory = $true)]$Process,
        [string]$StdoutPath,
        [string]$StderrPath,
        [string]$LatestReportPath,
        [DateTime]$StartedAtUtc,
        [int]$PollIntervalSec,
        [int]$StallTimeoutSec
    )
    $effectivePollIntervalSec = [Math]::Max([int]$PollIntervalSec, 1)
    $effectiveStallTimeoutSec = [Math]::Max([int]$StallTimeoutSec, 0)
    $progressPaths = @($StdoutPath, $StderrPath, $LatestReportPath) | Where-Object { -not [string]::IsNullOrWhiteSpace($_) }
    $lastFingerprint = Get-ProgressFingerprint -Paths $progressPaths
    $lastProgressAtUtc = [DateTime]::UtcNow

    while (-not $Process.HasExited) {
        Start-Sleep -Seconds $effectivePollIntervalSec
        try {
            $Process.Refresh()
        } catch {
        }
        $currentFingerprint = Get-ProgressFingerprint -Paths $progressPaths
        if ($currentFingerprint -ne $lastFingerprint) {
            $lastFingerprint = $currentFingerprint
            $lastProgressAtUtc = [DateTime]::UtcNow
        }
        $nowUtc = [DateTime]::UtcNow
        $runtimeSec = [int][Math]::Max(($nowUtc - $StartedAtUtc).TotalSeconds, 0)
        $idleSec = [int][Math]::Max(($nowUtc - $lastProgressAtUtc).TotalSeconds, 0)
        if ($effectiveStallTimeoutSec -gt 0 -and $idleSec -ge $effectiveStallTimeoutSec) {
            Stop-ProcessTreeBestEffort -ProcessId ([int]$Process.Id)
            Start-Sleep -Seconds 1
            try {
                $Process.Refresh()
            } catch {
            }
            return [PSCustomObject]@{
                ExitCode = if ($Process.HasExited) { [int]$Process.ExitCode } else { 124 }
                Stalled = $true
                RuntimeSec = $runtimeSec
                IdleSec = $idleSec
                LastProgressAtUtc = $lastProgressAtUtc
                PollIntervalSec = $effectivePollIntervalSec
                StallTimeoutSec = $effectiveStallTimeoutSec
                ReasonCodes = @("HUNG_PROCESS")
                ProgressPaths = @($progressPaths)
            }
        }
    }

    $finishedAtUtc = [DateTime]::UtcNow
    return [PSCustomObject]@{
        ExitCode = [int]$Process.ExitCode
        Stalled = $false
        RuntimeSec = [int][Math]::Max(($finishedAtUtc - $StartedAtUtc).TotalSeconds, 0)
        IdleSec = [int][Math]::Max(($finishedAtUtc - $lastProgressAtUtc).TotalSeconds, 0)
        LastProgressAtUtc = $lastProgressAtUtc
        PollIntervalSec = $effectivePollIntervalSec
        StallTimeoutSec = $effectiveStallTimeoutSec
        ReasonCodes = @()
        ProgressPaths = @($progressPaths)
    }
}

function Resolve-ReportedJsonPathFromLog {
    param(
        [string]$LogPath,
        [string]$LogTag
    )
    if ([string]::IsNullOrWhiteSpace($LogPath) -or [string]::IsNullOrWhiteSpace($LogTag) -or (-not (Test-Path $LogPath))) {
        return ""
    }
    $pattern = '^\[' + [Regex]::Escape($LogTag) + '\] report=(.+)$'
    $match = Get-Content -Path $LogPath -Encoding UTF8 | Select-String -Pattern $pattern | Select-Object -Last 1
    if ($null -eq $match) {
        return ""
    }
    $reportedPath = [string]$match.Matches[0].Groups[1].Value
    if ([string]::IsNullOrWhiteSpace($reportedPath)) {
        return ""
    }
    return $reportedPath.Trim()
}

function Resolve-FreshLatestJsonPath {
    param(
        [string]$LatestPath,
        [DateTime]$StartedAtUtc
    )
    if ([string]::IsNullOrWhiteSpace($LatestPath) -or (-not (Test-Path $LatestPath))) {
        return ""
    }
    $item = Get-Item -Path $LatestPath -ErrorAction SilentlyContinue
    if ($null -eq $item) {
        return ""
    }
    if ($item.LastWriteTimeUtc -lt $StartedAtUtc.AddSeconds(-2)) {
        return ""
    }
    return $item.FullName
}

function Load-JsonReportOrEmpty {
    param([string]$PathValue)
    if ([string]::IsNullOrWhiteSpace($PathValue) -or (-not (Test-Path $PathValue))) {
        return @{}
    }
    $raw = Get-Content -Path $PathValue -Raw -Encoding UTF8
    if ([string]::IsNullOrWhiteSpace($raw)) {
        return @{}
    }
    return $raw | ConvertFrom-Json
}

function To-Bool {
    param(
        [Parameter(Mandatory = $false)]$Value,
        [bool]$DefaultValue = $false
    )
    if ($null -eq $Value) {
        return $DefaultValue
    }
    try {
        return [bool]$Value
    } catch {
        return $DefaultValue
    }
}

function Get-StringArray {
    param([Parameter(Mandatory = $false)]$Value)
    if ($null -eq $Value) {
        return @()
    }
    return @($Value | ForEach-Object { [string]$_ })
}

function Test-NonFatalScoutReport {
    param([Parameter(Mandatory = $false)]$AcceptanceReport)
    $reasons = Get-StringArray -Value (Get-PropValue -ObjectValue $AcceptanceReport -Name "reasons" -DefaultValue @())
    if ($reasons -contains "SCOUT_ONLY_BUDGET_EVIDENCE") {
        return $true
    }
    $backtestGate = Get-PropValue -ObjectValue (Get-PropValue -ObjectValue $AcceptanceReport -Name "gates" -DefaultValue @{}) -Name "backtest" -DefaultValue @{}
    $budgetReasons = Get-StringArray -Value (Get-PropValue -ObjectValue $backtestGate -Name "budget_contract_reasons" -DefaultValue @())
    return ($budgetReasons -contains "SCOUT_ONLY_BUDGET_EVIDENCE")
}

$resolvedProjectRoot = if ([string]::IsNullOrWhiteSpace($ProjectRoot)) { Resolve-DefaultProjectRoot } else { $ProjectRoot }
$resolvedProjectRoot = [System.IO.Path]::GetFullPath($resolvedProjectRoot)
$resolvedPythonExe = if ([string]::IsNullOrWhiteSpace($PythonExe)) { Resolve-DefaultPythonExe -Root $resolvedProjectRoot } else { $PythonExe }
$resolvedDailyPipelineScript = if ([string]::IsNullOrWhiteSpace($DailyPipelineScript)) { Resolve-DefaultDailyPipelineScript -Root $resolvedProjectRoot } else { $DailyPipelineScript }
$resolvedV3AcceptanceScript = if ([string]::IsNullOrWhiteSpace($V3AcceptanceScript)) { Resolve-DefaultV3AcceptanceScript -Root $resolvedProjectRoot } else { $V3AcceptanceScript }
$resolvedV4AcceptanceScript = if ([string]::IsNullOrWhiteSpace($V4AcceptanceScript)) { Resolve-DefaultV4AcceptanceScript -Root $resolvedProjectRoot } else { $V4AcceptanceScript }
$effectiveBatchDate = if ([string]::IsNullOrWhiteSpace($BatchDate)) { (Get-Date).Date.AddDays(-1).ToString("yyyy-MM-dd") } else { $BatchDate }
$effectiveBatchDate = Resolve-DateToken -DateText $effectiveBatchDate -LabelForError "batch_date"
$batchDateObj = [DateTime]::ParseExact($effectiveBatchDate, "yyyy-MM-dd", [System.Globalization.CultureInfo]::InvariantCulture)
$v3FeatureStartDate = $batchDateObj.AddDays(-1 * [Math]::Max($V3TrainLookbackDays - 1, 0)).ToString("yyyy-MM-dd")
$v4FeatureStartDate = $batchDateObj.AddDays(-1 * [Math]::Max($V4TrainLookbackDays - 1, 0)).ToString("yyyy-MM-dd")
$psExe = Resolve-PwshExe
$logsDir = Join-Path $resolvedProjectRoot "logs/daily_parallel_acceptance"
New-Item -ItemType Directory -Path $logsDir -Force | Out-Null

$report = [ordered]@{
    generated_at = (Get-Date).ToString("o")
    batch_date = $effectiveBatchDate
    steps = [ordered]@{}
    lanes = [ordered]@{}
    monitoring = [ordered]@{
        lane_poll_interval_sec = [int]([Math]::Max($LanePollIntervalSec, 1))
        lane_stall_timeout_sec = [int]([Math]::Max($LaneStallTimeoutSec, 0))
    }
    overall_pass = $false
}

try {
    if (-not $SkipDailyPipeline) {
        $dailyArgs = @(
            "-NoProfile",
            "-ExecutionPolicy", "Bypass",
            "-File", $resolvedDailyPipelineScript,
            "-ProjectRoot", $resolvedProjectRoot,
            "-PythonExe", $resolvedPythonExe,
            "-Date", $effectiveBatchDate,
            "-SkipSmoke"
        )
        $dailyExec = Invoke-CommandCapture -Exe $psExe -ArgList $dailyArgs
        $report.steps.daily_pipeline = [ordered]@{
            attempted = $true
            exit_code = [int]$dailyExec.ExitCode
            command = $dailyExec.Command
            output_preview = (Get-OutputPreview -Text ([string]$dailyExec.Output))
        }
        if ($dailyExec.ExitCode -ne 0) {
            throw "shared daily pipeline failed"
        }
    } else {
        $report.steps.daily_pipeline = [ordered]@{ attempted = $false; reason = "SKIPPED_BY_FLAG" }
    }

    if (-not $SkipFeaturesBuild) {
        $v3BuildArgs = @(
            "-m", "autobot.cli",
            "features", "build",
            "--feature-set", "v3",
            "--tf", $Tf,
            "--quote", $Quote,
            "--top-n", $TrainTopN,
            "--start", $v3FeatureStartDate,
            "--end", $effectiveBatchDate
        )
        $v3Build = Invoke-CommandCapture -Exe $resolvedPythonExe -ArgList $v3BuildArgs
        $report.steps.features_v3 = [ordered]@{
            attempted = $true
            exit_code = [int]$v3Build.ExitCode
            command = $v3Build.Command
            output_preview = (Get-OutputPreview -Text ([string]$v3Build.Output))
            start = $v3FeatureStartDate
            end = $effectiveBatchDate
        }
        if ($v3Build.ExitCode -ne 0) {
            throw "features build v3 failed"
        }

        $v4BuildArgs = @(
            "-m", "autobot.cli",
            "features", "build",
            "--feature-set", "v4",
            "--label-set", "v2",
            "--tf", $Tf,
            "--quote", $Quote,
            "--top-n", $TrainTopN,
            "--start", $v4FeatureStartDate,
            "--end", $effectiveBatchDate
        )
        $v4Build = Invoke-CommandCapture -Exe $resolvedPythonExe -ArgList $v4BuildArgs
        $report.steps.features_v4 = [ordered]@{
            attempted = $true
            exit_code = [int]$v4Build.ExitCode
            command = $v4Build.Command
            output_preview = (Get-OutputPreview -Text ([string]$v4Build.Output))
            start = $v4FeatureStartDate
            end = $effectiveBatchDate
        }
        if ($v4Build.ExitCode -ne 0) {
            throw "features build v4 failed"
        }
    } else {
        $report.steps.features_v3 = [ordered]@{ attempted = $false; reason = "SKIPPED_BY_FLAG" }
        $report.steps.features_v4 = [ordered]@{ attempted = $false; reason = "SKIPPED_BY_FLAG" }
    }

    $laneProcesses = @()
    if (-not $SkipV3) {
        $v3Args = @(
            "-NoProfile",
            "-ExecutionPolicy", "Bypass",
            "-File", $resolvedV3AcceptanceScript,
            "-ProjectRoot", $resolvedProjectRoot,
            "-PythonExe", $resolvedPythonExe,
            "-BatchDate", $effectiveBatchDate,
            "-SkipDailyPipeline",
            "-SkipReportRefresh"
        )
        if ($DryRun) {
            $v3Exec = Invoke-CommandCapture -Exe $psExe -ArgList $v3Args
            $report.lanes.v3 = [ordered]@{
                attempted = $true
                dry_run = $true
                exit_code = [int]$v3Exec.ExitCode
                command = $v3Exec.Command
                output_preview = (Get-OutputPreview -Text ([string]$v3Exec.Output))
            }
        } else {
            $laneProcesses += [PSCustomObject]@{
                name = "v3"
                log_tag = "v3-accept"
                latest_report_path = (Join-Path $resolvedProjectRoot "logs/model_v3_acceptance/latest.json")
                meta = Start-AcceptanceProcess -PwshExe $psExe -ArgList $v3Args -LogPrefix "v3_accept" -LogDir $logsDir -WorkingDirectory $resolvedProjectRoot
            }
        }
    } else {
        $report.lanes.v3 = [ordered]@{ attempted = $false; reason = "SKIPPED_BY_FLAG" }
    }

    if (-not $SkipV4) {
        $v4Args = @(
            "-NoProfile",
            "-ExecutionPolicy", "Bypass",
            "-File", $resolvedV4AcceptanceScript,
            "-ProjectRoot", $resolvedProjectRoot,
            "-PythonExe", $resolvedPythonExe,
            "-BatchDate", $effectiveBatchDate,
            "-SkipDailyPipeline",
            "-SkipReportRefresh"
        )
        if ($DryRun) {
            $v4Exec = Invoke-CommandCapture -Exe $psExe -ArgList $v4Args
            $report.lanes.v4 = [ordered]@{
                attempted = $true
                dry_run = $true
                exit_code = [int]$v4Exec.ExitCode
                command = $v4Exec.Command
                output_preview = (Get-OutputPreview -Text ([string]$v4Exec.Output))
            }
        } else {
            $laneProcesses += [PSCustomObject]@{
                name = "v4"
                log_tag = "v4-accept"
                latest_report_path = (Join-Path $resolvedProjectRoot "logs/model_v4_acceptance/latest.json")
                meta = Start-AcceptanceProcess -PwshExe $psExe -ArgList $v4Args -LogPrefix "v4_accept" -LogDir $logsDir -WorkingDirectory $resolvedProjectRoot
            }
        }
    } else {
        $report.lanes.v4 = [ordered]@{ attempted = $false; reason = "SKIPPED_BY_FLAG" }
    }

    if (-not $DryRun) {
        foreach ($lane in $laneProcesses) {
            $waitResult = Wait-AcceptanceProcessWithWatchdog `
                -LaneName $lane.name `
                -Process $lane.meta.Process `
                -StdoutPath $lane.meta.StdoutPath `
                -StderrPath $lane.meta.StderrPath `
                -LatestReportPath $lane.latest_report_path `
                -StartedAtUtc $lane.meta.StartedAtUtc `
                -PollIntervalSec $LanePollIntervalSec `
                -StallTimeoutSec $LaneStallTimeoutSec
            $latestReportPath = $lane.latest_report_path
            $runReportPath = Resolve-ReportedJsonPathFromLog -LogPath $lane.meta.StdoutPath -LogTag $lane.log_tag
            $freshLatestPath = Resolve-FreshLatestJsonPath -LatestPath $latestReportPath -StartedAtUtc $lane.meta.StartedAtUtc
            $effectiveReportPath = if (-not [string]::IsNullOrWhiteSpace($runReportPath) -and (Test-Path $runReportPath)) {
                $runReportPath
            } else {
                $freshLatestPath
            }
            $effectiveReport = Load-JsonReportOrEmpty -PathValue $effectiveReportPath
            $latestOverallPass = To-Bool (
                Get-PropValue -ObjectValue (Get-PropValue -ObjectValue $effectiveReport -Name "gates" -DefaultValue @{}) -Name "overall_pass" -DefaultValue $false
            ) $false
            $latestReasons = @(Get-PropValue -ObjectValue $effectiveReport -Name "reasons" -DefaultValue @())
            if ($waitResult.Stalled) {
                $latestReasons = @($latestReasons) + @($waitResult.ReasonCodes)
            }
            $nonfatalScoutRejection = Test-NonFatalScoutReport -AcceptanceReport $effectiveReport
            $report.lanes[$lane.name] = [ordered]@{
                attempted = $true
                exit_code = [int]$waitResult.ExitCode
                command = $lane.meta.Command
                stdout_log = $lane.meta.StdoutPath
                stderr_log = $lane.meta.StderrPath
                run_report_path = $runReportPath
                latest_report_path = $latestReportPath
                effective_report_path = $effectiveReportPath
                effective_report_source = if ($effectiveReportPath -eq $runReportPath) { "run_report" } elseif (-not [string]::IsNullOrWhiteSpace($effectiveReportPath)) { "latest_fresh_fallback" } else { "missing" }
                latest_overall_pass = [bool]$latestOverallPass
                latest_reasons = @($latestReasons)
                nonfatal_scout_rejection = [bool]$nonfatalScoutRejection
                watchdog_stalled = [bool]$waitResult.Stalled
                watchdog_reason_codes = @($waitResult.ReasonCodes)
                watchdog_runtime_sec = [int]$waitResult.RuntimeSec
                watchdog_idle_sec = [int]$waitResult.IdleSec
                watchdog_last_progress_at_utc = if ($waitResult.LastProgressAtUtc) { ([DateTime]$waitResult.LastProgressAtUtc).ToString("o") } else { $null }
                watchdog_poll_interval_sec = [int]$waitResult.PollIntervalSec
                watchdog_stall_timeout_sec = [int]$waitResult.StallTimeoutSec
            }
        }
    }

    $v3Pass = if ($report.lanes.Contains("v3")) {
        if ($report.lanes.v3.attempted -eq $false) {
            $true
        } else {
            $v3LatestOverallPass = Get-PropValue -ObjectValue $report.lanes.v3 -Name "latest_overall_pass" -DefaultValue $false
            $v3WatchdogStalled = Get-PropValue -ObjectValue $report.lanes.v3 -Name "watchdog_stalled" -DefaultValue $false
            [bool]((-not $v3WatchdogStalled) -and ($v3LatestOverallPass -or ($report.lanes.v3.dry_run -eq $true -and $report.lanes.v3.exit_code -eq 0)))
        }
    } else { $true }
    $v4Pass = if ($report.lanes.Contains("v4")) {
        if ($report.lanes.v4.attempted -eq $false) {
            $true
        } else {
            $v4LatestOverallPass = Get-PropValue -ObjectValue $report.lanes.v4 -Name "latest_overall_pass" -DefaultValue $false
            $v4NonfatalScoutRejection = Get-PropValue -ObjectValue $report.lanes.v4 -Name "nonfatal_scout_rejection" -DefaultValue $false
            $v4WatchdogStalled = Get-PropValue -ObjectValue $report.lanes.v4 -Name "watchdog_stalled" -DefaultValue $false
            [bool]((-not $v4WatchdogStalled) -and ($v4LatestOverallPass -or $v4NonfatalScoutRejection -or ($report.lanes.v4.dry_run -eq $true -and $report.lanes.v4.exit_code -eq 0)))
        }
    } else { $true }
    $report.overall_pass = $v3Pass -and $v4Pass
} catch {
    $report.overall_pass = $false
    $report.exception = [ordered]@{
        message = $_.Exception.Message
    }
}

$reportPath = Join-Path $logsDir ("daily_parallel_acceptance_" + (Get-Date -Format "yyyyMMdd-HHmmss") + ".json")
$latestPath = Join-Path $logsDir "latest.json"
$reportJson = $report | ConvertTo-Json -Depth 8
$reportJson | Set-Content -Path $reportPath -Encoding UTF8
$reportJson | Set-Content -Path $latestPath -Encoding UTF8

Write-Host ("[daily-parallel] batch_date={0}" -f $effectiveBatchDate)
Write-Host ("[daily-parallel] report={0}" -f $reportPath)
Write-Host ("[daily-parallel] latest={0}" -f $latestPath)
Write-Host ("[daily-parallel] overall_pass={0}" -f $report.overall_pass)

if ($report.overall_pass) {
    exit 0
}
exit 2
