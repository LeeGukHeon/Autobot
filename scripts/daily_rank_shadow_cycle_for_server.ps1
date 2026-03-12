param(
    [string]$ProjectRoot = "",
    [string]$PythonExe = "",
    [string]$AcceptanceScript = "",
    [string]$BatchDate = "",
    [string[]]$BlockOnActiveUnits = @(),
    [string[]]$AcceptanceArgs = @(),
    [switch]$SkipDailyPipeline,
    [switch]$SkipReportRefresh,
    [switch]$DryRun
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

. (Join-Path $PSScriptRoot "systemd_service_utils.ps1")

function Resolve-DefaultAcceptanceScript {
    param([string]$Root)
    return (Join-Path $Root "scripts/v4_rank_shadow_candidate_acceptance.ps1")
}

function Resolve-ReportedJsonPath {
    param([string]$OutputText)
    $regex = [System.Text.RegularExpressions.Regex]::new("(?m)^\[[^\]]+\]\s+report=(.+)$")
    $match = $regex.Match([string]$OutputText)
    if (-not $match.Success) {
        return ""
    }
    return [string]$match.Groups[1].Value.Trim()
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

function Write-JsonFile {
    param(
        [string]$PathValue,
        [Parameter(Mandatory = $true)]$Payload
    )
    $parent = Split-Path -Parent $PathValue
    if (-not [string]::IsNullOrWhiteSpace($parent)) {
        New-Item -ItemType Directory -Force -Path $parent | Out-Null
    }
    $Payload | ConvertTo-Json -Depth 10 | Set-Content -Path $PathValue -Encoding UTF8
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
    try {
        $property = $ObjectValue.PSObject.Properties[$Name]
        if ($null -ne $property) {
            return $property.Value
        }
    } catch {
    }
    return $DefaultValue
}

function Test-ObjectHasValues {
    param([Parameter(Mandatory = $false)]$ObjectValue)
    if ($null -eq $ObjectValue) {
        return $false
    }
    if ($ObjectValue -is [System.Collections.IDictionary]) {
        return ($ObjectValue.Count -gt 0)
    }
    if ($ObjectValue.PSObject) {
        return (@($ObjectValue.PSObject.Properties).Count -gt 0)
    }
    return $true
}

function Get-StringArray {
    param([Parameter(Mandatory = $false)]$Value)
    return @(Expand-DelimitedStringArray -Value $Value)
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

function Test-SystemdUnitActive {
    param([string]$UnitName)
    if ($DryRun) {
        return $false
    }
    $systemctl = Get-Command systemctl -ErrorAction SilentlyContinue
    if ($null -eq $systemctl) {
        return $false
    }
    & $systemctl.Source is-active --quiet $UnitName
    return ($LASTEXITCODE -eq 0)
}

function Invoke-CommandCapture {
    param(
        [string]$Exe,
        [string[]]$ArgList,
        [switch]$AllowFailure
    )
    $commandText = $Exe + " " + (($ArgList | ForEach-Object { Quote-ShellArg ([string]$_) }) -join " ")
    if ($DryRun) {
        Write-Host ("[rank-shadow-cycle][dry-run] {0}" -f $commandText)
        return [PSCustomObject]@{
            ExitCode = 0
            Output = "[dry-run] $commandText"
            Command = $commandText
        }
    }
    $output = & $Exe @ArgList 2>&1
    $exitCode = [int]$LASTEXITCODE
    if ((-not $AllowFailure) -and $exitCode -ne 0) {
        throw ("command failed: " + $commandText)
    }
    return [PSCustomObject]@{
        ExitCode = $exitCode
        Output = ($output -join "`n")
        Command = $commandText
    }
}

function Test-AcceptanceFatalFailure {
    param(
        [int]$ExitCode,
        [Parameter(Mandatory = $false)]$AcceptanceReport
    )
    if ($ExitCode -eq 0) {
        return $false
    }
    if (($ExitCode -ne 2) -or (-not (Test-ObjectHasValues -ObjectValue $AcceptanceReport))) {
        return $true
    }
    $steps = Get-PropValue -ObjectValue $AcceptanceReport -Name "steps" -DefaultValue @{}
    $exceptionStep = Get-PropValue -ObjectValue $steps -Name "exception" -DefaultValue @{}
    if (Test-ObjectHasValues -ObjectValue $exceptionStep) {
        return $true
    }
    $reasons = Get-StringArray -Value (Get-PropValue -ObjectValue $AcceptanceReport -Name "reasons" -DefaultValue @())
    foreach ($reason in $reasons) {
        if (@(
            "UNHANDLED_EXCEPTION",
            "DAILY_PIPELINE_FAILED",
            "TRAIN_OR_CANDIDATE_POINTER_FAILED"
        ) -contains [string]$reason) {
            return $true
        }
    }
    return $false
}

function Build-RankShadowCycleReport {
    param(
        [string]$BatchDateValue,
        [string]$AcceptanceScriptPath,
        [string]$AcceptanceReportPath,
        [int]$AcceptanceExitCode,
        [string]$AcceptanceCommand,
        [Parameter(Mandatory = $false)]$AcceptanceReport,
        [string[]]$BlockingUnits = @(),
        [bool]$SkippedBecauseBlocked = $false
    )
    $reasons = Get-StringArray -Value (Get-PropValue -ObjectValue $AcceptanceReport -Name "reasons" -DefaultValue @())
    $notes = Get-StringArray -Value (Get-PropValue -ObjectValue $AcceptanceReport -Name "notes" -DefaultValue @())
    $fatal = Test-AcceptanceFatalFailure -ExitCode $AcceptanceExitCode -AcceptanceReport $AcceptanceReport
    $candidate = if ($fatal) { @{} } else { Get-PropValue -ObjectValue $AcceptanceReport -Name "candidate" -DefaultValue @{} }
    $config = if ($fatal) { @{} } else { Get-PropValue -ObjectValue $AcceptanceReport -Name "config" -DefaultValue @{} }
    $gates = if ($fatal) { @{} } else { Get-PropValue -ObjectValue $AcceptanceReport -Name "gates" -DefaultValue @{} }
    $backtestGate = if ($fatal) { @{} } else { Get-PropValue -ObjectValue $gates -Name "backtest" -DefaultValue @{} }
    $overallPass = [bool](Get-PropValue -ObjectValue $gates -Name "overall_pass" -DefaultValue $false)
    $laneShadowOnly = [bool](Get-PropValue -ObjectValue $candidate -Name "lane_shadow_only" -DefaultValue $false)
    $decisionBasis = [string](Get-PropValue -ObjectValue $backtestGate -Name "decision_basis" -DefaultValue "")

    $status = "shadow_hold"
    $nextAction = "use_cls_primary_lane"
    $actionReason = if ([string]::IsNullOrWhiteSpace($decisionBasis)) { "ACCEPTANCE_NOT_PASSING" } else { $decisionBasis }
    if ($SkippedBecauseBlocked) {
        $status = "skipped"
        $nextAction = "preserve_previous_lane_action"
        $actionReason = "PRIMARY_LANE_ACTIVE"
    } elseif ($fatal) {
        $status = "fatal_error"
        $nextAction = "use_cls_primary_lane"
        $actionReason = "FATAL_ACCEPTANCE_FAILURE"
    } elseif ($overallPass -and $laneShadowOnly) {
        $status = "shadow_pass"
        $nextAction = "use_rank_governed_lane"
        $actionReason = "SHADOW_EVIDENCE_READY"
    } elseif ($overallPass) {
        $status = "pass_without_shadow_guard"
        $nextAction = "use_cls_primary_lane"
        $actionReason = "NONSTANDARD_PASS"
    }

    return [ordered]@{
        version = 1
        policy = "v4_rank_shadow_cycle_v1"
        generated_at = (Get-Date).ToString("o")
        batch_date = $BatchDateValue
        acceptance_script = $AcceptanceScriptPath
        acceptance_report_path = $AcceptanceReportPath
        acceptance_exit_code = [int]$AcceptanceExitCode
        acceptance_command = $AcceptanceCommand
        status = $status
        next_action = $nextAction
        action_reason = $actionReason
        candidate_run_id = [string](Get-PropValue -ObjectValue $candidate -Name "run_id" -DefaultValue "")
        candidate_run_dir = [string](Get-PropValue -ObjectValue $candidate -Name "run_dir" -DefaultValue "")
        task = [string](Get-PropValue -ObjectValue $config -Name "task" -DefaultValue "")
        lane_id = [string](Get-PropValue -ObjectValue $candidate -Name "lane_id" -DefaultValue "")
        lane_role = [string](Get-PropValue -ObjectValue $candidate -Name "lane_role" -DefaultValue "")
        lane_shadow_only = $laneShadowOnly
        lane_promotion_allowed = [bool](Get-PropValue -ObjectValue $candidate -Name "lane_promotion_allowed" -DefaultValue $false)
        overall_pass = $overallPass
        backtest_pass = [bool](Get-PropValue -ObjectValue $backtestGate -Name "pass" -DefaultValue $false)
        decision_basis = $decisionBasis
        reasons = @($reasons)
        notes = @($notes)
        blocked_units = @($BlockingUnits)
    }
}

function Build-GovernanceActionArtifact {
    param(
        [Parameter(Mandatory = $true)]$CycleReport,
        [Parameter(Mandatory = $false)]$PreviousAction
    )
    $cycleStatus = [string](Get-PropValue -ObjectValue $CycleReport -Name "status" -DefaultValue "")
    $nextAction = [string](Get-PropValue -ObjectValue $CycleReport -Name "next_action" -DefaultValue "")
    $reason = [string](Get-PropValue -ObjectValue $CycleReport -Name "action_reason" -DefaultValue "")
    $selectedLaneId = "cls_primary"
    $selectedScript = "v4_promotable_candidate_acceptance.ps1"
    $automationMode = "default_cls"

    if ($nextAction -eq "use_rank_governed_lane") {
        $selectedLaneId = "rank_governed_primary"
        $selectedScript = "v4_rank_governed_candidate_acceptance.ps1"
        $automationMode = "rank_shadow_auto_pass"
    } elseif ($nextAction -eq "preserve_previous_lane_action" -and (Test-ObjectHasValues -ObjectValue $PreviousAction)) {
        $selectedLaneId = [string](Get-PropValue -ObjectValue $PreviousAction -Name "selected_lane_id" -DefaultValue "cls_primary")
        $selectedScript = [string](Get-PropValue -ObjectValue $PreviousAction -Name "selected_acceptance_script" -DefaultValue "v4_promotable_candidate_acceptance.ps1")
        $automationMode = "preserved_previous_action"
        if ([string]::IsNullOrWhiteSpace($reason)) {
            $reason = "PRIMARY_LANE_ACTIVE"
        }
    }

    return [ordered]@{
        version = 1
        policy = "v4_rank_shadow_governance_action_v1"
        generated_at = (Get-Date).ToString("o")
        source_cycle_status = $cycleStatus
        source_cycle_action = $nextAction
        source_cycle_report_path = [string](Get-PropValue -ObjectValue $CycleReport -Name "acceptance_report_path" -DefaultValue "")
        source_rank_shadow_cycle_path = ""
        selected_lane_id = $selectedLaneId
        selected_acceptance_script = $selectedScript
        automation_mode = $automationMode
        reason = $reason
        candidate_run_id = [string](Get-PropValue -ObjectValue $CycleReport -Name "candidate_run_id" -DefaultValue "")
    }
}

$resolvedProjectRoot = if ([string]::IsNullOrWhiteSpace($ProjectRoot)) { Resolve-DefaultProjectRoot } else { $ProjectRoot }
$resolvedProjectRoot = [System.IO.Path]::GetFullPath($resolvedProjectRoot)
$resolvedPythonExe = if ([string]::IsNullOrWhiteSpace($PythonExe)) { Resolve-DefaultPythonExe -Root $resolvedProjectRoot } else { $PythonExe }
$resolvedAcceptanceScript = if ([string]::IsNullOrWhiteSpace($AcceptanceScript)) { Resolve-DefaultAcceptanceScript -Root $resolvedProjectRoot } else { $AcceptanceScript }
$resolvedBatchDate = Resolve-BatchDateValue -DateText $BatchDate
$resolvedBlockOnActiveUnits = @(Get-StringArray -Value $BlockOnActiveUnits)
$resolvedAcceptanceArgs = @(Get-StringArray -Value $AcceptanceArgs)
$cycleRoot = Join-Path $resolvedProjectRoot "logs/model_v4_rank_shadow_cycle"
$runReportPath = Join-Path $cycleRoot ("rank_shadow_cycle_" + (Get-Date -Format "yyyyMMdd-HHmmss") + ".json")
$latestReportPath = Join-Path $cycleRoot "latest.json"
$governedCandidatePath = Join-Path $cycleRoot "latest_governed_candidate.json"
$governanceActionPath = Join-Path $cycleRoot "latest_governance_action.json"
$previousGovernanceAction = Load-JsonOrEmpty -PathValue $governanceActionPath

$activeBlockUnits = @()
foreach ($unit in $resolvedBlockOnActiveUnits) {
    $trimmed = [string]$unit
    if ([string]::IsNullOrWhiteSpace($trimmed)) {
        continue
    }
    if (Test-SystemdUnitActive -UnitName $trimmed.Trim()) {
        $activeBlockUnits += $trimmed.Trim()
    }
}

if ($activeBlockUnits.Count -gt 0) {
    $blockedReport = Build-RankShadowCycleReport `
        -BatchDateValue $resolvedBatchDate `
        -AcceptanceScriptPath $resolvedAcceptanceScript `
        -AcceptanceReportPath "" `
        -AcceptanceExitCode 0 `
        -AcceptanceCommand "" `
        -AcceptanceReport @{} `
        -BlockingUnits $activeBlockUnits `
        -SkippedBecauseBlocked $true
    if (-not $DryRun) {
        $governanceAction = Build-GovernanceActionArtifact -CycleReport $blockedReport -PreviousAction $previousGovernanceAction
        $governanceAction.source_rank_shadow_cycle_path = $runReportPath
        Write-JsonFile -PathValue $runReportPath -Payload $blockedReport
        Write-JsonFile -PathValue $latestReportPath -Payload $blockedReport
        Write-JsonFile -PathValue $governanceActionPath -Payload $governanceAction
    }
    Write-Host ("[rank-shadow-cycle] status=skipped")
    Write-Host ("[rank-shadow-cycle] blocked_units={0}" -f ($activeBlockUnits -join ","))
    Write-Host ("[rank-shadow-cycle] report={0}" -f $runReportPath)
    Write-Host ("[rank-shadow-cycle] latest={0}" -f $latestReportPath)
    exit 0
}

$psExe = if ([System.IO.Path]::DirectorySeparatorChar -eq '\') { "powershell.exe" } else { Resolve-PwshExe }
$argList = @(
    "-NoProfile",
    "-ExecutionPolicy", "Bypass",
    "-File", $resolvedAcceptanceScript,
    "-ProjectRoot", $resolvedProjectRoot,
    "-PythonExe", $resolvedPythonExe,
    "-BatchDate", $resolvedBatchDate
)
if ($SkipDailyPipeline) {
    $argList += "-SkipDailyPipeline"
}
if ($SkipReportRefresh) {
    $argList += "-SkipReportRefresh"
}
if ($DryRun) {
    $argList += "-DryRun"
}
$argList += $resolvedAcceptanceArgs

$commandPreview = $psExe + " " + (($argList | ForEach-Object { Quote-ShellArg ([string]$_) }) -join " ")
Write-Host ("[rank-shadow-cycle] batch_date={0}" -f $resolvedBatchDate)
Write-Host ("[rank-shadow-cycle] acceptance_script={0}" -f $resolvedAcceptanceScript)
Write-Host ("[rank-shadow-cycle] command={0}" -f $commandPreview)

$exec = Invoke-CommandCapture -Exe $psExe -ArgList $argList -AllowFailure
$acceptanceReportPath = Resolve-ReportedJsonPath -OutputText $exec.Output
$acceptanceReport = Load-JsonOrEmpty -PathValue $acceptanceReportPath
$cycleReport = Build-RankShadowCycleReport `
    -BatchDateValue $resolvedBatchDate `
    -AcceptanceScriptPath $resolvedAcceptanceScript `
    -AcceptanceReportPath $acceptanceReportPath `
    -AcceptanceExitCode ([int]$exec.ExitCode) `
    -AcceptanceCommand $exec.Command `
    -AcceptanceReport $acceptanceReport
$governanceAction = Build-GovernanceActionArtifact -CycleReport $cycleReport -PreviousAction $previousGovernanceAction
$governanceAction.source_rank_shadow_cycle_path = $runReportPath

if (-not $DryRun) {
    $cycleReport.governance_action = $governanceAction
    Write-JsonFile -PathValue $runReportPath -Payload $cycleReport
    Write-JsonFile -PathValue $latestReportPath -Payload $cycleReport
    Write-JsonFile -PathValue $governanceActionPath -Payload $governanceAction
    if ([string](Get-PropValue -ObjectValue $cycleReport -Name "status" -DefaultValue "") -eq "shadow_pass") {
        Write-JsonFile -PathValue $governedCandidatePath -Payload $cycleReport
    } elseif (Test-Path $governedCandidatePath) {
        Remove-Item -Path $governedCandidatePath -Force -ErrorAction SilentlyContinue
    }
}

Write-Host ("[rank-shadow-cycle] status={0}" -f ([string](Get-PropValue -ObjectValue $cycleReport -Name "status" -DefaultValue "")))
Write-Host ("[rank-shadow-cycle] next_action={0}" -f ([string](Get-PropValue -ObjectValue $cycleReport -Name "next_action" -DefaultValue "")))
Write-Host ("[rank-shadow-cycle] governance_action={0}" -f ([string](Get-PropValue -ObjectValue $governanceAction -Name "selected_lane_id" -DefaultValue "")))
Write-Host ("[rank-shadow-cycle] report={0}" -f $runReportPath)
Write-Host ("[rank-shadow-cycle] latest={0}" -f $latestReportPath)

if (Test-AcceptanceFatalFailure -ExitCode ([int]$exec.ExitCode) -AcceptanceReport $acceptanceReport) {
    exit ([int]$exec.ExitCode)
}
exit 0
