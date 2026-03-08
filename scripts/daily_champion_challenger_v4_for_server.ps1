param(
    [string]$ProjectRoot = "",
    [string]$PythonExe = "",
    [string]$AcceptanceScript = "",
    [string]$RuntimeInstallScript = "",
    [string]$BatchDate = "",
    [string]$ChampionUnitName = "autobot-paper-v4.service",
    [string]$ChallengerUnitName = "autobot-paper-v4-challenger.service",
    [string[]]$PromotionTargetUnits = @(),
    [double]$ChallengerMinHours = 12.0,
    [int]$ChallengerMinOrdersFilled = 2,
    [double]$ChallengerMinRealizedPnlQuote = 0.0,
    [double]$ChallengerMinMicroQualityScore = 0.25,
    [double]$ChallengerMinNonnegativeRatio = 0.34,
    [double]$ChallengerMaxDrawdownDeteriorationFactor = 1.10,
    [double]$ChallengerMicroQualityTolerance = 0.02,
    [double]$ChallengerNonnegativeRatioTolerance = 0.05,
    [ValidateSet("combined", "promote_only", "spawn_only")]
    [string]$Mode = "combined",
    [switch]$SkipDailyPipeline,
    [switch]$SkipReportRefresh,
    [switch]$DryRun
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

. (Join-Path $PSScriptRoot "systemd_service_utils.ps1")

function Resolve-DefaultAcceptanceScript {
    param([string]$Root)
    return (Join-Path $Root "scripts/v4_candidate_acceptance.ps1")
}

function Resolve-DefaultRuntimeInstallScript {
    param([string]$Root)
    return (Join-Path $Root "scripts/install_server_runtime_services.ps1")
}

function Resolve-ChampionRunId {
    param([string]$Root)
    $pointerPath = Join-Path $Root "models/registry/train_v4_crypto_cs/champion.json"
    $pointer = Load-JsonOrEmpty -PathValue $pointerPath
    return [string](Get-PropValue -ObjectValue $pointer -Name "run_id" -DefaultValue "")
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
    $output = & $Exe @ArgList 2>&1
    $exitCode = $LASTEXITCODE
    if ((-not $AllowFailure) -and $exitCode -ne 0) {
        throw ("command failed: " + $Exe + " " + ($ArgList -join " "))
    }
    return [PSCustomObject]@{
        ExitCode = [int]$exitCode
        Output = [string]($output -join [Environment]::NewLine)
        Command = ($Exe + " " + (($ArgList | ForEach-Object { Quote-ShellArg ([string]$_) }) -join " "))
    }
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

function Write-JsonFile {
    param(
        [string]$PathValue,
        $Payload
    )
    $parent = Split-Path -Path $PathValue -Parent
    if (-not [string]::IsNullOrWhiteSpace($parent)) {
        New-Item -ItemType Directory -Force -Path $parent | Out-Null
    }
    $json = $Payload | ConvertTo-Json -Depth 20
    Set-Content -Path $PathValue -Value $json -Encoding UTF8
}

function Stop-UnitIfActive {
    param([string]$UnitName)
    if ([string]::IsNullOrWhiteSpace($UnitName)) {
        return $false
    }
    $wasActive = Test-SystemdUnitActive -UnitName $UnitName
    if ($wasActive -and (-not $DryRun)) {
        & sudo systemctl stop $UnitName
        if ($LASTEXITCODE -ne 0) {
            throw "failed to stop unit: $UnitName"
        }
    }
    return $wasActive
}

function Restart-Unit {
    param([string]$UnitName)
    if ([string]::IsNullOrWhiteSpace($UnitName) -or $DryRun) {
        return
    }
    & sudo systemctl restart $UnitName
    if ($LASTEXITCODE -ne 0) {
        throw "failed to restart unit: $UnitName"
    }
}

function Start-OrUpdate-ChallengerUnit {
    param(
        [string]$RuntimeInstallScriptPath,
        [string]$Root,
        [string]$PyExe,
        [string]$UnitName,
        [string]$CandidateRunId
    )
    $psExe = Resolve-PwshExe
    $args = @(
        "-NoProfile",
        "-ExecutionPolicy", "Bypass",
        "-File", $RuntimeInstallScriptPath,
        "-ProjectRoot", $Root,
        "-PythonExe", $PyExe,
        "-PaperUnitName", $UnitName,
        "-PaperPreset", "live_v4",
        "-PaperRuntimeRole", "challenger",
        "-PaperLaneName", "v4",
        "-PaperModelRefPinned", $CandidateRunId,
        "-NoBootstrapChampion",
        "-NoEnable",
        "-PaperCliArgs",
        "--model-ref",
        $CandidateRunId
    )
    return Invoke-CommandCapture -Exe $psExe -ArgList $args
}

$resolvedProjectRoot = if ([string]::IsNullOrWhiteSpace($ProjectRoot)) { Resolve-DefaultProjectRoot } else { $ProjectRoot }
$resolvedProjectRoot = [System.IO.Path]::GetFullPath($resolvedProjectRoot)
$resolvedPythonExe = if ([string]::IsNullOrWhiteSpace($PythonExe)) { Resolve-DefaultPythonExe -Root $resolvedProjectRoot } else { $PythonExe }
$resolvedAcceptanceScript = if ([string]::IsNullOrWhiteSpace($AcceptanceScript)) { Resolve-DefaultAcceptanceScript -Root $resolvedProjectRoot } else { $AcceptanceScript }
$resolvedRuntimeInstallScript = if ([string]::IsNullOrWhiteSpace($RuntimeInstallScript)) { Resolve-DefaultRuntimeInstallScript -Root $resolvedProjectRoot } else { $RuntimeInstallScript }
$resolvedBatchDate = Resolve-BatchDateValue -DateText $BatchDate
$stateRoot = Join-Path $resolvedProjectRoot "logs/model_v4_challenger"
$statePath = Join-Path $stateRoot "current_state.json"
$archiveRoot = Join-Path $stateRoot "archive"
$reportPath = Join-Path $stateRoot ("daily_loop_" + (Get-Date -Format "yyyyMMdd-HHmmss") + ".json")
$latestReportPath = Join-Path $stateRoot "latest.json"
$psExe = Resolve-PwshExe
$runPromotionPhase = $Mode -ne "spawn_only"
$runSpawnPhase = $Mode -ne "promote_only"

$report = [ordered]@{
    mode = $Mode
    batch_date = $resolvedBatchDate
    started_at_utc = (Get-Date).ToUniversalTime().ToString("o")
    champion_unit = $ChampionUnitName
    challenger_unit = $ChallengerUnitName
    promotion_target_units = @($PromotionTargetUnits)
    steps = [ordered]@{}
    challenger_previous = @{}
    challenger_next = @{}
}
$candidateRunId = ""

$previousState = Load-JsonOrEmpty -PathValue $statePath
$hasPreviousState = Test-ObjectHasValues -ObjectValue $previousState
$challengerWasActive = Test-SystemdUnitActive -UnitName $ChallengerUnitName
$championWasActive = Test-SystemdUnitActive -UnitName $ChampionUnitName
$report.steps.unit_snapshot = [ordered]@{
    challenger_was_active = $challengerWasActive
    champion_was_active = $championWasActive
    previous_state_present = $hasPreviousState
}

if (($Mode -eq "spawn_only") -and $hasPreviousState) {
    $staleCandidateRunId = [string](Get-PropValue -ObjectValue $previousState -Name "candidate_run_id" -DefaultValue "")
    $report.steps.spawn_guard = [ordered]@{
        triggered = $true
        reason = "PREVIOUS_CHALLENGER_STATE_PRESENT"
        candidate_run_id = $staleCandidateRunId
    }
    $report.steps.promote_previous_challenger = [ordered]@{
        attempted = $false
        reason = "SKIPPED_BY_MODE"
    }
    $report.steps.train_candidate = [ordered]@{
        attempted = $false
        reason = "PREVIOUS_CHALLENGER_STATE_PRESENT"
    }
    $report.steps.start_challenger = [ordered]@{
        attempted = $false
        reason = "PREVIOUS_CHALLENGER_STATE_PRESENT"
        candidate_run_id = $staleCandidateRunId
    }
    $report.completed_at_utc = (Get-Date).ToUniversalTime().ToString("o")
    if (-not $DryRun) {
        Write-JsonFile -PathValue $reportPath -Payload $report
        Write-JsonFile -PathValue $latestReportPath -Payload $report
    }
    Write-Host ("[daily-cc][error] mode={0} reason=PREVIOUS_CHALLENGER_STATE_PRESENT" -f $Mode)
    Write-Host ("[daily-cc] batch_date={0}" -f $resolvedBatchDate)
    Write-Host ("[daily-cc] report={0}" -f $reportPath)
    Write-Host ("[daily-cc] latest={0}" -f $latestReportPath)
    Write-Host ("[daily-cc] challenger_candidate_run_id={0}" -f $staleCandidateRunId)
    exit 2
}

$challengerStopped = $false
if (($runPromotionPhase -or $runSpawnPhase) -and $challengerWasActive) {
    $challengerStopped = Stop-UnitIfActive -UnitName $ChallengerUnitName
}
$report.steps.stop_units = [ordered]@{
    challenger_was_active = $challengerWasActive
    challenger_stopped = $challengerStopped
    champion_was_active = $championWasActive
    champion_stopped = $false
}

$promotionPerformed = $false
$promotionDecision = @{}
if ($runPromotionPhase) {
    if ($hasPreviousState) {
        $candidateRunId = [string](Get-PropValue -ObjectValue $previousState -Name "candidate_run_id" -DefaultValue "")
        $championRunIdAtStart = [string](Get-PropValue -ObjectValue $previousState -Name "champion_run_id_at_start" -DefaultValue "")
        $startedTsMs = [int64](Get-PropValue -ObjectValue $previousState -Name "started_ts_ms" -DefaultValue 0)
        if ((-not [string]::IsNullOrWhiteSpace($candidateRunId)) -and ($startedTsMs -gt 0)) {
            $compareArgs = @(
                "-m", "autobot.common.paper_lane_evidence",
                "--paper-root", (Join-Path $resolvedProjectRoot "data/paper"),
                "--lane", "v4",
                "--challenger-model-ref", $candidateRunId,
                "--champion-model-run-id", $championRunIdAtStart,
                "--since-ts-ms", [string]$startedTsMs,
                "--min-challenger-hours", [string]$ChallengerMinHours,
                "--min-orders-filled", [string]$ChallengerMinOrdersFilled,
                "--min-realized-pnl-quote", [string]$ChallengerMinRealizedPnlQuote,
                "--min-micro-quality-score", [string]$ChallengerMinMicroQualityScore,
                "--min-nonnegative-ratio", [string]$ChallengerMinNonnegativeRatio,
                "--max-drawdown-deterioration-factor", [string]$ChallengerMaxDrawdownDeteriorationFactor,
                "--micro-quality-tolerance", [string]$ChallengerMicroQualityTolerance,
                "--nonnegative-ratio-tolerance", [string]$ChallengerNonnegativeRatioTolerance
            )
            $compareExec = Invoke-CommandCapture -Exe $resolvedPythonExe -ArgList $compareArgs
            $promotionDecision = $compareExec.Output | ConvertFrom-Json
            $report.challenger_previous = $promotionDecision
            if (-not $DryRun) {
                New-Item -ItemType Directory -Force -Path $archiveRoot | Out-Null
                $archivePath = Join-Path $archiveRoot ("challenger_" + (Get-Date -Format "yyyyMMdd-HHmmss") + "_" + $candidateRunId + ".json")
                Write-JsonFile -PathValue $archivePath -Payload ([ordered]@{
                    state = $previousState
                    comparison = $promotionDecision
                })
            }
            $shouldPromote = [bool](Get-PropValue -ObjectValue (Get-PropValue -ObjectValue $promotionDecision -Name "decision" -DefaultValue @{}) -Name "promote" -DefaultValue $false)
            if ($shouldPromote -and (-not $DryRun)) {
                $promoteExec = Invoke-CommandCapture -Exe $resolvedPythonExe -ArgList @(
                    "-m", "autobot.cli",
                    "model", "promote",
                    "--model-ref", $candidateRunId,
                    "--model-family", "train_v4_crypto_cs"
                )
                $promotionPerformed = $true
                $restartedUnits = New-Object System.Collections.Generic.List[string]
                Restart-Unit -UnitName $ChampionUnitName
                $restartedUnits.Add($ChampionUnitName) | Out-Null
                foreach ($unit in @($PromotionTargetUnits)) {
                    $trimmedUnit = [string]$unit
                    if ([string]::IsNullOrWhiteSpace($trimmedUnit)) {
                        continue
                    }
                    if ($trimmedUnit -eq $ChampionUnitName) {
                        continue
                    }
                    if (Test-SystemdUnitActive -UnitName $trimmedUnit) {
                        Restart-Unit -UnitName $trimmedUnit
                        $restartedUnits.Add($trimmedUnit) | Out-Null
                    }
                }
                $report.steps.promote_previous_challenger = [ordered]@{
                    attempted = $true
                    command = $promoteExec.Command
                    output_preview = $promoteExec.Output
                    promoted = $true
                    candidate_run_id = $candidateRunId
                    restarted_units = @($restartedUnits)
                }
            } else {
                $report.steps.promote_previous_challenger = [ordered]@{
                    attempted = $false
                    promoted = $false
                    candidate_run_id = $candidateRunId
                    reason = if ($shouldPromote) { "DRY_RUN" } else { [string](Get-PropValue -ObjectValue (Get-PropValue -ObjectValue $promotionDecision -Name "decision" -DefaultValue @{}) -Name "decision" -DefaultValue "keep_champion") }
                }
            }
        } else {
            $report.steps.promote_previous_challenger = [ordered]@{
                attempted = $false
                promoted = $false
                candidate_run_id = $candidateRunId
                reason = "PREVIOUS_STATE_INCOMPLETE"
            }
        }
        if (-not $DryRun) {
            Remove-Item -Path $statePath -Force -ErrorAction SilentlyContinue
        }
    } else {
        $report.steps.promote_previous_challenger = [ordered]@{
            attempted = $false
            promoted = $false
            reason = "NO_PREVIOUS_CHALLENGER_STATE"
        }
    }
} else {
    $report.steps.promote_previous_challenger = [ordered]@{
        attempted = $false
        reason = "SKIPPED_BY_MODE"
    }
}

$championRestartReason = ""
if (-not $DryRun) {
    if ($promotionPerformed) {
        $championRestartReason = "PROMOTED_NEW_CHAMPION"
    } elseif (-not $championWasActive) {
        Restart-Unit -UnitName $ChampionUnitName
        $championRestartReason = "CHAMPION_WAS_INACTIVE"
    }
}
$report.steps.champion_runtime = [ordered]@{
    was_active_at_start = $championWasActive
    restart_reason = if ([string]::IsNullOrWhiteSpace($championRestartReason)) { "UNCHANGED" } else { $championRestartReason }
}

if ($runSpawnPhase) {
    $acceptArgs = @(
        "-NoProfile",
        "-ExecutionPolicy", "Bypass",
        "-File", $resolvedAcceptanceScript,
        "-ProjectRoot", $resolvedProjectRoot,
        "-PythonExe", $resolvedPythonExe,
        "-BatchDate", $resolvedBatchDate,
        "-SkipPaperSoak",
        "-SkipPromote"
    )
    if ($SkipDailyPipeline) {
        $acceptArgs += "-SkipDailyPipeline"
    }
    if ($SkipReportRefresh) {
        $acceptArgs += "-SkipReportRefresh"
    }
    if ($DryRun) {
        $acceptArgs += "-DryRun"
    }

    $acceptExec = Invoke-CommandCapture -Exe $psExe -ArgList $acceptArgs
    $acceptReportPath = Resolve-ReportedJsonPath -OutputText $acceptExec.Output
    $acceptReport = Load-JsonOrEmpty -PathValue $acceptReportPath
    $candidateRunId = [string](Get-PropValue -ObjectValue (Get-PropValue -ObjectValue (Get-PropValue -ObjectValue $acceptReport -Name "steps" -DefaultValue @{}) -Name "train" -DefaultValue @{}) -Name "candidate_run_id" -DefaultValue "")
    $backtestPass = [bool](Get-PropValue -ObjectValue (Get-PropValue -ObjectValue (Get-PropValue -ObjectValue $acceptReport -Name "gates" -DefaultValue @{}) -Name "backtest" -DefaultValue @{}) -Name "pass" -DefaultValue $false)
    $overallPass = [bool](Get-PropValue -ObjectValue (Get-PropValue -ObjectValue $acceptReport -Name "gates" -DefaultValue @{}) -Name "overall_pass" -DefaultValue $false)
    $report.steps.train_candidate = [ordered]@{
        command = $acceptExec.Command
        output_preview = $acceptExec.Output
        report_path = $acceptReportPath
        candidate_run_id = $candidateRunId
        backtest_pass = $backtestPass
        overall_pass = $overallPass
    }

    if ((-not [string]::IsNullOrWhiteSpace($candidateRunId)) -and $backtestPass) {
        $challengerInstallExec = Start-OrUpdate-ChallengerUnit `
            -RuntimeInstallScriptPath $resolvedRuntimeInstallScript `
            -Root $resolvedProjectRoot `
            -PyExe $resolvedPythonExe `
            -UnitName $ChallengerUnitName `
            -CandidateRunId $candidateRunId
        $report.steps.start_challenger = [ordered]@{
            command = $challengerInstallExec.Command
            output_preview = $challengerInstallExec.Output
            candidate_run_id = $candidateRunId
        }
        $championRunIdAtStart = Resolve-ChampionRunId -Root $resolvedProjectRoot
        $nextState = [ordered]@{
            batch_date = $resolvedBatchDate
            candidate_run_id = $candidateRunId
            champion_ref_at_start = "champion_v4"
            champion_run_id_at_start = $championRunIdAtStart
            started_ts_ms = [int64](Get-Date -UFormat %s) * 1000
            started_at_utc = (Get-Date).ToUniversalTime().ToString("o")
            champion_unit = $ChampionUnitName
            challenger_unit = $ChallengerUnitName
            promotion_target_units = @($PromotionTargetUnits)
        }
        if (-not $DryRun) {
            Write-JsonFile -PathValue $statePath -Payload $nextState
        }
        $report.challenger_next = $nextState
    } else {
        $report.steps.start_challenger = [ordered]@{
            skipped = $true
            candidate_run_id = $candidateRunId
            reason = if ([string]::IsNullOrWhiteSpace($candidateRunId)) { "NO_CANDIDATE_RUN_ID" } elseif (-not $backtestPass) { "BACKTEST_SANITY_FAILED" } else { "UNKNOWN" }
        }
        if (-not $DryRun) {
            & sudo systemctl stop $ChallengerUnitName 2>$null
            Remove-Item -Path $statePath -Force -ErrorAction SilentlyContinue
        }
    }
} else {
    $report.steps.train_candidate = [ordered]@{
        attempted = $false
        reason = "SKIPPED_BY_MODE"
    }
    $report.steps.start_challenger = [ordered]@{
        attempted = $false
        reason = "SKIPPED_BY_MODE"
    }
}

$report.completed_at_utc = (Get-Date).ToUniversalTime().ToString("o")
if (-not $DryRun) {
    Write-JsonFile -PathValue $reportPath -Payload $report
    Write-JsonFile -PathValue $latestReportPath -Payload $report
}

Write-Host ("[daily-cc] mode={0}" -f $Mode)
Write-Host ("[daily-cc] batch_date={0}" -f $resolvedBatchDate)
Write-Host ("[daily-cc] report={0}" -f $reportPath)
Write-Host ("[daily-cc] latest={0}" -f $latestReportPath)
Write-Host ("[daily-cc] challenger_candidate_run_id={0}" -f $candidateRunId)
exit 0
