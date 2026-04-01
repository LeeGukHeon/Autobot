param(
    [string]$ProjectRoot = "",
    [string]$PythonExe = "",
    [string]$BatchDate = "",
    [string]$TrainingCriticalStartDate = "",
    [string]$TrainingCriticalEndDate = "",
    [string]$TrainSnapshotCloseScript = "",
    [string]$GovernedAcceptanceScript = "",
    [string]$NightlyChainScript = "",
    [string]$SummaryPath = "logs/ops/v5_fusion_train_window_recovery/latest.json",
    [switch]$DryRun
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

. (Join-Path $PSScriptRoot "systemd_service_utils.ps1")

function Resolve-ProjectPath {
    param(
        [string]$Root,
        [string]$PathValue
    )
    if ([string]::IsNullOrWhiteSpace($PathValue)) {
        return $Root
    }
    if ([System.IO.Path]::IsPathRooted($PathValue)) {
        return [System.IO.Path]::GetFullPath($PathValue)
    }
    return [System.IO.Path]::GetFullPath((Join-Path $Root $PathValue))
}

function Load-JsonOrEmpty {
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

function Resolve-BatchDateValue {
    param([string]$DateText)
    if (-not [string]::IsNullOrWhiteSpace($DateText)) {
        return [DateTime]::ParseExact(
            $DateText,
            "yyyy-MM-dd",
            [System.Globalization.CultureInfo]::InvariantCulture,
            [System.Globalization.DateTimeStyles]::None
        ).ToString("yyyy-MM-dd")
    }
    return (Get-Date).Date.AddDays(-1).ToString("yyyy-MM-dd")
}

function Format-CommandLine {
    param(
        [string]$Exe,
        [string[]]$ArgList
    )
    return ($Exe + " " + (($ArgList | ForEach-Object { Quote-ShellArg ([string]$_) }) -join " "))
}

function Invoke-CommandCapture {
    param(
        [string]$Exe,
        [string[]]$ArgList,
        [switch]$AllowFailure
    )
    $commandText = Format-CommandLine -Exe $Exe -ArgList $ArgList
    Write-Host ("[recover-v5-fusion] command={0}" -f $commandText)
    if ($DryRun) {
        return [PSCustomObject]@{
            ExitCode = 0
            Output = ""
            Command = $commandText
        }
    }
    $output = & $Exe @ArgList 2>&1
    $exitCode = [int]$LASTEXITCODE
    $outputText = [string]($output -join [Environment]::NewLine)
    if (-not [string]::IsNullOrWhiteSpace($outputText)) {
        Write-Host $outputText
    }
    if ((-not $AllowFailure) -and $exitCode -ne 0) {
        throw ("command failed: " + $commandText)
    }
    return [PSCustomObject]@{
        ExitCode = $exitCode
        Output = $outputText
        Command = $commandText
    }
}

function Save-RecoverySummary {
    param(
        [string]$ResolvedSummaryPath,
        [hashtable]$Payload
    )
    $summaryDir = Split-Path -Parent $ResolvedSummaryPath
    if (-not [string]::IsNullOrWhiteSpace($summaryDir)) {
        New-Item -ItemType Directory -Force -Path $summaryDir | Out-Null
    }
    $runPath = Join-Path $summaryDir ("recovery_" + (Get-Date -Format "yyyyMMdd-HHmmss") + ".json")
    ($Payload | ConvertTo-Json -Depth 12) | Set-Content -Path $ResolvedSummaryPath -Encoding UTF8
    ($Payload | ConvertTo-Json -Depth 12) | Set-Content -Path $runPath -Encoding UTF8
    return [PSCustomObject]@{
        LatestSummaryPath = $ResolvedSummaryPath
        RunSummaryPath = $runPath
    }
}

$resolvedProjectRoot = if ([string]::IsNullOrWhiteSpace($ProjectRoot)) { Resolve-DefaultProjectRoot } else { [System.IO.Path]::GetFullPath($ProjectRoot) }
$resolvedPythonExe = if ([string]::IsNullOrWhiteSpace($PythonExe)) { Resolve-DefaultPythonExe -Root $resolvedProjectRoot } else { $PythonExe }
$resolvedPwshExe = Resolve-PwshExe
$resolvedBatchDate = Resolve-BatchDateValue -DateText $BatchDate
$resolvedSummaryPath = Resolve-ProjectPath -Root $resolvedProjectRoot -PathValue $SummaryPath
$resolvedTrainSnapshotCloseScript = if ([string]::IsNullOrWhiteSpace($TrainSnapshotCloseScript)) { Join-Path $resolvedProjectRoot "scripts/close_v5_train_ready_snapshot.ps1" } else { Resolve-ProjectPath -Root $resolvedProjectRoot -PathValue $TrainSnapshotCloseScript }
$resolvedGovernedAcceptanceScript = if ([string]::IsNullOrWhiteSpace($GovernedAcceptanceScript)) { Join-Path $resolvedProjectRoot "scripts/v5_governed_candidate_acceptance.ps1" } else { Resolve-ProjectPath -Root $resolvedProjectRoot -PathValue $GovernedAcceptanceScript }
$resolvedNightlyChainScript = if ([string]::IsNullOrWhiteSpace($NightlyChainScript)) { Join-Path $resolvedProjectRoot "scripts/daily_champion_challenger_v5_for_server.ps1" } else { Resolve-ProjectPath -Root $resolvedProjectRoot -PathValue $NightlyChainScript }
$resolvedTrainingCriticalStartDate = if ([string]::IsNullOrWhiteSpace($TrainingCriticalStartDate)) { "" } else { Resolve-BatchDateValue -DateText $TrainingCriticalStartDate }
$resolvedTrainingCriticalEndDate = if ([string]::IsNullOrWhiteSpace($TrainingCriticalEndDate)) { $resolvedBatchDate } else { Resolve-BatchDateValue -DateText $TrainingCriticalEndDate }

$summary = [ordered]@{
    policy = "v5_fusion_train_window_recovery_v1"
    started_at_utc = (Get-Date).ToUniversalTime().ToString("o")
    project_root = $resolvedProjectRoot
    python_exe = $resolvedPythonExe
    batch_date = $resolvedBatchDate
    training_critical_start_date = $resolvedTrainingCriticalStartDate
    training_critical_end_date = $resolvedTrainingCriticalEndDate
    dry_run = [bool]$DryRun
    steps = [ordered]@{}
    overall_pass = $false
    failure_stage = ""
    failure_code = ""
    failure_report_path = ""
}

try {
    $closeArgs = @(
        "-NoProfile",
        "-ExecutionPolicy", "Bypass",
        "-File", $resolvedTrainSnapshotCloseScript,
        "-ProjectRoot", $resolvedProjectRoot,
        "-PythonExe", $resolvedPythonExe,
        "-BatchDate", $resolvedBatchDate,
        "-TrainingCriticalStartDate", $resolvedTrainingCriticalStartDate,
        "-TrainingCriticalEndDate", $resolvedTrainingCriticalEndDate,
        "-SkipDeadline"
    )
    if ($DryRun) {
        $closeArgs += "-DryRun"
    }
    $closeExec = Invoke-CommandCapture -Exe $resolvedPwshExe -ArgList $closeArgs -AllowFailure
    $closeReportPath = Join-Path $resolvedProjectRoot "data/collect/_meta/train_snapshot_close_latest.json"
    $closeReport = Load-JsonOrEmpty -PathValue $closeReportPath
    $summary.steps.train_snapshot_close = [ordered]@{
        attempted = $true
        exit_code = [int]$closeExec.ExitCode
        command = [string]$closeExec.Command
        report_path = $closeReportPath
        overall_pass = [bool](Get-PropValue -ObjectValue $closeReport -Name "overall_pass" -DefaultValue $false)
    }
    if ([int]$closeExec.ExitCode -ne 0) {
        $summary.failure_stage = "data_close"
        $summary.failure_code = "TRAIN_SNAPSHOT_CLOSE_FAILED"
        $summary.failure_report_path = $closeReportPath
        throw "train snapshot close failed"
    }

    $acceptArgs = @(
        "-NoProfile",
        "-ExecutionPolicy", "Bypass",
        "-File", $resolvedGovernedAcceptanceScript,
        "-ProjectRoot", $resolvedProjectRoot,
        "-PythonExe", $resolvedPythonExe,
        "-BatchDate", $resolvedBatchDate,
        "-SkipDailyPipeline",
        "-SkipPaperSoak",
        "-SkipPromote"
    )
    if ($DryRun) {
        $acceptArgs += "-DryRun"
    }
    $acceptExec = Invoke-CommandCapture -Exe $resolvedPwshExe -ArgList $acceptArgs -AllowFailure
    $acceptReportPath = Join-Path $resolvedProjectRoot "logs/model_v5_acceptance/latest.json"
    $acceptReport = Load-JsonOrEmpty -PathValue $acceptReportPath
    $summary.steps.recovery_acceptance = [ordered]@{
        attempted = $true
        exit_code = [int]$acceptExec.ExitCode
        command = [string]$acceptExec.Command
        report_path = $acceptReportPath
        overall_pass = Get-PropValue -ObjectValue $acceptReport -Name "overall_pass" -DefaultValue $null
        reasons = @((Get-PropValue -ObjectValue $acceptReport -Name "reasons" -DefaultValue @()))
        failure_stage = [string](Get-PropValue -ObjectValue $acceptReport -Name "failure_stage" -DefaultValue "")
        failure_code = [string](Get-PropValue -ObjectValue $acceptReport -Name "failure_code" -DefaultValue "")
        failure_report_path = [string](Get-PropValue -ObjectValue $acceptReport -Name "failure_report_path" -DefaultValue "")
    }
    if ([int]$acceptExec.ExitCode -ne 0) {
        $summary.failure_stage = [string](Get-PropValue -ObjectValue $acceptReport -Name "failure_stage" -DefaultValue "acceptance_gate")
        $summary.failure_code = [string](Get-PropValue -ObjectValue $acceptReport -Name "failure_code" -DefaultValue "RECOVERY_ACCEPTANCE_FAILED")
        $summary.failure_report_path = [string](Get-PropValue -ObjectValue $acceptReport -Name "failure_report_path" -DefaultValue $acceptReportPath)
        throw "recovery acceptance failed"
    }

    $nightlyArgs = @(
        "-NoProfile",
        "-ExecutionPolicy", "Bypass",
        "-File", $resolvedNightlyChainScript,
        "-ProjectRoot", $resolvedProjectRoot,
        "-PythonExe", $resolvedPythonExe,
        "-BatchDate", $resolvedBatchDate,
        "-Mode", "spawn_only"
    )
    if ($DryRun) {
        $nightlyArgs += "-DryRun"
    }
    $nightlyExec = Invoke-CommandCapture -Exe $resolvedPwshExe -ArgList $nightlyArgs -AllowFailure
    $nightlyReportPath = Join-Path $resolvedProjectRoot "logs/model_v5_candidate/latest.json"
    $nightlyReport = Load-JsonOrEmpty -PathValue $nightlyReportPath
    $summary.steps.nightly_chain_rerun = [ordered]@{
        attempted = $true
        exit_code = [int]$nightlyExec.ExitCode
        command = [string]$nightlyExec.Command
        report_path = $nightlyReportPath
        failure_stage = [string](Get-PropValue -ObjectValue $nightlyReport -Name "failure_stage" -DefaultValue "")
        failure_code = [string](Get-PropValue -ObjectValue $nightlyReport -Name "failure_code" -DefaultValue "")
        failure_report_path = [string](Get-PropValue -ObjectValue $nightlyReport -Name "failure_report_path" -DefaultValue "")
    }
    if ([int]$nightlyExec.ExitCode -ne 0) {
        $summary.failure_stage = [string](Get-PropValue -ObjectValue $nightlyReport -Name "failure_stage" -DefaultValue "acceptance_gate")
        $summary.failure_code = [string](Get-PropValue -ObjectValue $nightlyReport -Name "failure_code" -DefaultValue "NIGHTLY_CHAIN_RERUN_FAILED")
        $summary.failure_report_path = [string](Get-PropValue -ObjectValue $nightlyReport -Name "failure_report_path" -DefaultValue $nightlyReportPath)
        throw "nightly chain rerun failed"
    }

    $summary.overall_pass = $true
    $summary.final_acceptance_report_path = $acceptReportPath
    $summary.final_nightly_report_path = $nightlyReportPath
} catch {
    if ([string]::IsNullOrWhiteSpace([string]$summary.failure_code)) {
        $summary.failure_code = "UNHANDLED_EXCEPTION"
    }
    if ([string]::IsNullOrWhiteSpace([string]$summary.failure_stage)) {
        $summary.failure_stage = "acceptance_gate"
    }
    $summary.exception = [ordered]@{
        message = [string]$_.Exception.Message
        type = if ($null -eq $_.Exception) { "" } else { [string]$_.Exception.GetType().FullName }
    }
} finally {
    $summary.completed_at_utc = (Get-Date).ToUniversalTime().ToString("o")
    $saved = Save-RecoverySummary -ResolvedSummaryPath $resolvedSummaryPath -Payload $summary
    Write-Host ("[recover-v5-fusion] latest={0}" -f $saved.LatestSummaryPath)
    Write-Host ("[recover-v5-fusion] report={0}" -f $saved.RunSummaryPath)
}

if (-not [bool]$summary.overall_pass) {
    exit 2
}
exit 0
