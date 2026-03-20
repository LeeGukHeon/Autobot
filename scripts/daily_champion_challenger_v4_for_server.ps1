param(
    [string]$ProjectRoot = "",
    [string]$PythonExe = "",
    [string]$AcceptanceScript = "",
    [string]$RuntimeInstallScript = "",
    [string]$ExecutionPolicyRefreshScript = "",
    [string]$BatchDate = "",
    [string]$ChampionUnitName = "autobot-paper-v4.service",
    [string]$ChallengerUnitName = "autobot-paper-v4-challenger.service",
    [string[]]$PromotionTargetUnits = @(),
    [string[]]$CandidateTargetUnits = @(),
    [string[]]$BlockOnActiveUnits = @(),
    [string[]]$AcceptanceArgs = @(),
    [double]$ChallengerMinHours = 12.0,
    [int]$ChallengerMinOrdersFilled = 2,
    [double]$ChallengerMinRealizedPnlQuote = 0.0,
    [double]$ChallengerMinMicroQualityScore = 0.25,
    [double]$ChallengerMinNonnegativeRatio = 0.34,
    [double]$ChallengerMaxDrawdownDeteriorationFactor = 1.10,
    [double]$ChallengerMicroQualityTolerance = 0.02,
    [double]$ChallengerNonnegativeRatioTolerance = 0.05,
    [int]$ExecutionContractMinRows = 20,
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
    return (Join-Path $Root "scripts/v4_governed_candidate_acceptance.ps1")
}

function Resolve-DefaultRuntimeInstallScript {
    param([string]$Root)
    return (Join-Path $Root "scripts/install_server_runtime_services.ps1")
}

function Resolve-DefaultExecutionPolicyRefreshScript {
    param([string]$Root)
    return (Join-Path $Root "scripts/refresh_live_execution_policy.ps1")
}

function Resolve-ChampionRunId {
    param([string]$Root)
    $pointerPath = Join-Path $Root "models/registry/train_v4_crypto_cs/champion.json"
    $pointer = Load-JsonOrEmpty -PathValue $pointerPath
    return [string](Get-PropValue -ObjectValue $pointer -Name "run_id" -DefaultValue "")
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

function Resolve-LiveRolloutLatestPath {
    param(
        [string]$Root,
        [string]$UnitName = ""
    )
    $baseDir = Join-Path $Root "logs/live_rollout"
    $trimmedUnit = [string]$UnitName
    if (-not [string]::IsNullOrWhiteSpace($trimmedUnit)) {
        $slug = (($trimmedUnit.Trim().ToLowerInvariant()) -replace '[^a-z0-9]+', '_').Trim('_')
        if (-not [string]::IsNullOrWhiteSpace($slug)) {
            $scopedPath = Join-Path $baseDir ("latest." + $slug + ".json")
            if (Test-Path $scopedPath) {
                return $scopedPath
            }
        }
    }
    return (Join-Path $baseDir "latest.json")
}

function Resolve-PromotionTargetPolicy {
    param(
        [string]$Root,
        [string]$UnitName
    )
    $trimmedUnit = [string]$UnitName
    if ([string]::IsNullOrWhiteSpace($trimmedUnit)) {
        return [ordered]@{
            is_live_target = $false
            allowed = $false
            reason = "EMPTY_UNIT"
            contract = @{}
        }
    }
    $trimmedUnit = $trimmedUnit.Trim()
    $rolloutLatest = Load-JsonOrEmpty -PathValue (Resolve-LiveRolloutLatestPath -Root $Root -UnitName $trimmedUnit)
    $contract = Get-PropValue -ObjectValue $rolloutLatest -Name "contract" -DefaultValue @{}
    $contractTargetUnit = [string](Get-PropValue -ObjectValue $contract -Name "target_unit" -DefaultValue "")
    $contractMode = [string](Get-PropValue -ObjectValue $contract -Name "mode" -DefaultValue "")
    $contractArmed = [bool](Get-PropValue -ObjectValue $contract -Name "armed" -DefaultValue $false)
    $isLiveLikeUnit = $trimmedUnit.StartsWith("autobot-live")
    $isExplicitTarget = (-not [string]::IsNullOrWhiteSpace($contractTargetUnit)) -and ($contractTargetUnit -eq $trimmedUnit)
    if ((-not $isLiveLikeUnit) -and (-not $isExplicitTarget)) {
        return [ordered]@{
            is_live_target = $false
            allowed = $true
            reason = "NON_LIVE_TARGET"
            contract = $contract
        }
    }
    if (-not (Test-ObjectHasValues -ObjectValue $contract)) {
        return [ordered]@{
            is_live_target = $true
            allowed = $false
            reason = "LIVE_ROLLOUT_CONTRACT_MISSING"
            contract = @{}
        }
    }
    if ([string]::IsNullOrWhiteSpace($contractTargetUnit) -or ($contractTargetUnit -ne $trimmedUnit)) {
        return [ordered]@{
            is_live_target = $true
            allowed = $false
            reason = "LIVE_ROLLOUT_TARGET_MISMATCH"
            contract = $contract
        }
    }
    if (-not $contractArmed) {
        return [ordered]@{
            is_live_target = $true
            allowed = $false
            reason = "LIVE_ROLLOUT_NOT_ARMED"
            contract = $contract
        }
    }
    if (@("canary", "live") -notcontains $contractMode) {
        return [ordered]@{
            is_live_target = $true
            allowed = $false
            reason = "LIVE_ROLLOUT_MODE_NOT_PROMOTABLE"
            contract = $contract
        }
    }
    return [ordered]@{
        is_live_target = $true
        allowed = $true
        reason = "LIVE_ROLLOUT_ARMED"
        contract = $contract
    }
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

function Resolve-ExecutionContractRowsTotal {
    param([Parameter(Mandatory = $false)]$Payload)
    $executionContract = Get-PropValue -ObjectValue $Payload -Name "execution_contract" -DefaultValue @{}
    $rows = [int](Get-PropValue -ObjectValue $executionContract -Name "rows_total" -DefaultValue 0)
    if ($rows -gt 0) {
        return $rows
    }
    return [int](Get-PropValue -ObjectValue $Payload -Name "rows_total" -DefaultValue 0)
}

function Invoke-ExecutionContractRefresh {
    param(
        [string]$PwshExe,
        [string]$RefreshScriptPath,
        [string]$Root,
        [string]$PyExe,
        [switch]$IsDryRun
    )
    $args = @(
        "-NoProfile",
        "-ExecutionPolicy", "Bypass",
        "-File", $RefreshScriptPath,
        "-ProjectRoot", $Root,
        "-PythonExe", $PyExe
    )
    if ($IsDryRun) {
        $args += "-DryRun"
    }
    $exec = Invoke-CommandCapture -Exe $PwshExe -ArgList $args -AllowFailure
    $outputPath = ""
    if (-not [string]::IsNullOrWhiteSpace([string]$exec.Output)) {
        $lines = @([string]$exec.Output -split "\r?\n")
        for ($index = $lines.Count - 1; $index -ge 0; $index--) {
            $candidate = [string]$lines[$index]
            if ([string]::IsNullOrWhiteSpace($candidate)) {
                continue
            }
            $trimmed = $candidate.Trim()
            if ([string]::Equals([System.IO.Path]::GetExtension($trimmed), ".json", [System.StringComparison]::OrdinalIgnoreCase)) {
                $outputPath = $trimmed
                break
            }
        }
    }
    $artifact = Load-JsonOrEmpty -PathValue $outputPath
    return [PSCustomObject]@{
        ExitCode = [int]$exec.ExitCode
        Command = [string]$exec.Command
        Output = [string]$exec.Output
        OutputPath = [string]$outputPath
        Artifact = $artifact
        RowsTotal = [int](Resolve-ExecutionContractRowsTotal -Payload $artifact)
    }
}

function Stop-UnitIfActive {
    param([string]$UnitName)
    if ([string]::IsNullOrWhiteSpace($UnitName)) {
        return $false
    }
    $wasActive = Test-SystemdUnitActive -UnitName $UnitName -IsDryRun:$DryRun
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
        (Join-DelimitedStringArray -Values @("--model-ref", $CandidateRunId))
    )
    return Invoke-CommandCapture -Exe $psExe -ArgList $args
}

function Try-Restart-UnitBestEffort {
    param(
        [string]$UnitName,
        [System.Collections.Generic.List[string]]$RestoredUnits,
        [System.Collections.Generic.List[string]]$Errors
    )
    if ([string]::IsNullOrWhiteSpace($UnitName) -or $DryRun) {
        return
    }
    try {
        Restart-Unit -UnitName $UnitName
        if ($null -ne $RestoredUnits) {
            $RestoredUnits.Add($UnitName) | Out-Null
        }
    } catch {
        if ($null -ne $Errors) {
            $Errors.Add(("restart:{0}:{1}" -f $UnitName, $_.Exception.Message)) | Out-Null
        }
    }
}

function Try-Stop-UnitBestEffort {
    param(
        [string]$UnitName,
        [System.Collections.Generic.List[string]]$StoppedUnits,
        [System.Collections.Generic.List[string]]$Errors
    )
    if ([string]::IsNullOrWhiteSpace($UnitName) -or $DryRun) {
        return
    }
    try {
        & sudo systemctl stop $UnitName
        if ($LASTEXITCODE -ne 0) {
            throw "failed to stop unit: $UnitName"
        }
        if ($null -ne $StoppedUnits) {
            $StoppedUnits.Add($UnitName) | Out-Null
        }
    } catch {
        if ($null -ne $Errors) {
            $Errors.Add(("stop:{0}:{1}" -f $UnitName, $_.Exception.Message)) | Out-Null
        }
    }
}

function Invoke-RollbackOnFailure {
    $restoredUnits = New-Object System.Collections.Generic.List[string]
    $stoppedUnits = New-Object System.Collections.Generic.List[string]
    $removedPaths = New-Object System.Collections.Generic.List[string]
    $errors = New-Object System.Collections.Generic.List[string]
    $rollback = [ordered]@{
        attempted = $true
        repromoted_previous_champion = $false
        restored_units = @()
        stopped_units = @()
        removed_paths = @()
        errors = @()
    }
    if ($DryRun) {
        $rollback.reason = "DRY_RUN"
        return $rollback
    }

    if (-not [string]::IsNullOrWhiteSpace($statePath) -and (Test-Path $statePath)) {
        try {
            Remove-Item -Path $statePath -Force -ErrorAction Stop
            $removedPaths.Add($statePath) | Out-Null
        } catch {
            $errors.Add(("remove_state:{0}" -f $_.Exception.Message)) | Out-Null
        }
    }
    if (-not [string]::IsNullOrWhiteSpace($promoteCutoverLatestPath) -and (Test-Path $promoteCutoverLatestPath)) {
        try {
            Remove-Item -Path $promoteCutoverLatestPath -Force -ErrorAction Stop
            $removedPaths.Add($promoteCutoverLatestPath) | Out-Null
        } catch {
            $errors.Add(("remove_cutover_latest:{0}" -f $_.Exception.Message)) | Out-Null
        }
    }
    if (-not [string]::IsNullOrWhiteSpace($script:rollbackPromoteCutoverArchivePath) -and (Test-Path $script:rollbackPromoteCutoverArchivePath)) {
        try {
            Remove-Item -Path $script:rollbackPromoteCutoverArchivePath -Force -ErrorAction Stop
            $removedPaths.Add($script:rollbackPromoteCutoverArchivePath) | Out-Null
        } catch {
            $errors.Add(("remove_cutover_archive:{0}" -f $_.Exception.Message)) | Out-Null
        }
    }

    if ($script:rollbackPromotionPerformed) {
        if (-not [string]::IsNullOrWhiteSpace($script:rollbackPreviousChampionRunId)) {
            try {
                Invoke-CommandCapture -Exe $resolvedPythonExe -ArgList @(
                    "-m", "autobot.cli",
                    "model", "promote",
                    "--model-ref", $script:rollbackPreviousChampionRunId,
                    "--model-family", "train_v4_crypto_cs"
                ) | Out-Null
                $rollback.repromoted_previous_champion = $true
            } catch {
                $errors.Add(("repromote:{0}" -f $_.Exception.Message)) | Out-Null
            }
        }
        foreach ($unit in @($script:rollbackStartedInactivePromotionUnits.ToArray())) {
            Try-Stop-UnitBestEffort -UnitName $unit -StoppedUnits $stoppedUnits -Errors $errors
        }
        if ($script:rollbackChampionWasActive) {
            Try-Restart-UnitBestEffort -UnitName $ChampionUnitName -RestoredUnits $restoredUnits -Errors $errors
        }
        foreach ($unit in @($script:rollbackPreviouslyActivePromotionUnits.ToArray())) {
            if ([string]::IsNullOrWhiteSpace($unit) -or ($unit -eq $ChampionUnitName)) {
                continue
            }
            Try-Restart-UnitBestEffort -UnitName $unit -RestoredUnits $restoredUnits -Errors $errors
        }
    } elseif ($script:rollbackChallengerWasActive) {
        Try-Restart-UnitBestEffort -UnitName $ChallengerUnitName -RestoredUnits $restoredUnits -Errors $errors
    }

    $rollback.restored_units = @($restoredUnits.ToArray())
    $rollback.stopped_units = @($stoppedUnits.ToArray())
    $rollback.removed_paths = @($removedPaths.ToArray())
    $rollback.errors = @($errors.ToArray())
    return $rollback
}

$resolvedProjectRoot = if ([string]::IsNullOrWhiteSpace($ProjectRoot)) { Resolve-DefaultProjectRoot } else { $ProjectRoot }
$resolvedProjectRoot = [System.IO.Path]::GetFullPath($resolvedProjectRoot)
$resolvedPythonExe = if ([string]::IsNullOrWhiteSpace($PythonExe)) { Resolve-DefaultPythonExe -Root $resolvedProjectRoot } else { $PythonExe }
$resolvedAcceptanceScript = if ([string]::IsNullOrWhiteSpace($AcceptanceScript)) { Resolve-DefaultAcceptanceScript -Root $resolvedProjectRoot } else { $AcceptanceScript }
$resolvedRuntimeInstallScript = if ([string]::IsNullOrWhiteSpace($RuntimeInstallScript)) { Resolve-DefaultRuntimeInstallScript -Root $resolvedProjectRoot } else { $RuntimeInstallScript }
$resolvedExecutionPolicyRefreshScript = if ([string]::IsNullOrWhiteSpace($ExecutionPolicyRefreshScript)) { Resolve-DefaultExecutionPolicyRefreshScript -Root $resolvedProjectRoot } else { $ExecutionPolicyRefreshScript }
$resolvedBatchDate = Resolve-BatchDateValue -DateText $BatchDate
$resolvedPromotionTargetUnits = @(Get-StringArray -Value $PromotionTargetUnits)
$resolvedCandidateTargetUnits = @(Get-StringArray -Value $CandidateTargetUnits)
$resolvedBlockOnActiveUnits = @(Get-StringArray -Value $BlockOnActiveUnits)
$resolvedAcceptanceArgs = @(Get-StringArray -Value $AcceptanceArgs)
$stateRoot = Join-Path $resolvedProjectRoot "logs/model_v4_challenger"
$statePath = Join-Path $stateRoot "current_state.json"
$archiveRoot = Join-Path $stateRoot "archive"
$reportPath = Join-Path $stateRoot ("daily_loop_" + (Get-Date -Format "yyyyMMdd-HHmmss") + ".json")
$latestReportPath = Join-Path $stateRoot "latest.json"
$promoteCutoverLatestPath = Join-Path $stateRoot "latest_promote_cutover.json"
$promoteCutoverArchiveRoot = Join-Path $stateRoot "promote_cutover_archive"
$psExe = Resolve-PwshExe
$runPromotionPhase = $Mode -ne "spawn_only"
$runSpawnPhase = $Mode -ne "promote_only"

$report = [ordered]@{
    mode = $Mode
    batch_date = $resolvedBatchDate
    started_at_utc = (Get-Date).ToUniversalTime().ToString("o")
    champion_unit = $ChampionUnitName
    challenger_unit = $ChallengerUnitName
    promotion_target_units = @($resolvedPromotionTargetUnits)
    candidate_target_units = @($resolvedCandidateTargetUnits)
    steps = [ordered]@{}
    challenger_previous = @{}
    challenger_next = @{}
}
$candidateRunId = ""
$exitCode = 0
$script:rollbackPromotionPerformed = $false
$script:rollbackPreviousChampionRunId = ""
$script:rollbackChampionWasActive = $false
$script:rollbackChallengerWasActive = $false
$script:rollbackPromoteCutoverArchivePath = ""
$script:rollbackPreviouslyActivePromotionUnits = New-Object System.Collections.Generic.List[string]
$script:rollbackStartedInactivePromotionUnits = New-Object System.Collections.Generic.List[string]

trap {
    $exitCode = 2
    $report.exception = [ordered]@{
        message = $_.Exception.Message
    }
    $report.steps.rollback = Invoke-RollbackOnFailure
    $report.completed_at_utc = (Get-Date).ToUniversalTime().ToString("o")
    if (-not $DryRun) {
        Write-JsonFile -PathValue $reportPath -Payload $report
        Write-JsonFile -PathValue $latestReportPath -Payload $report
    }
    Write-Host ("[daily-cc][error] mode={0} reason={1}" -f $Mode, $_.Exception.Message)
    Write-Host ("[daily-cc] batch_date={0}" -f $resolvedBatchDate)
    Write-Host ("[daily-cc] report={0}" -f $reportPath)
    Write-Host ("[daily-cc] latest={0}" -f $latestReportPath)
    Write-Host ("[daily-cc] challenger_candidate_run_id={0}" -f $candidateRunId)
    exit $exitCode
}

$previousState = Load-JsonOrEmpty -PathValue $statePath
$hasPreviousState = Test-ObjectHasValues -ObjectValue $previousState
$challengerWasActive = Test-SystemdUnitActive -UnitName $ChallengerUnitName -IsDryRun:$DryRun
$championWasActive = Test-SystemdUnitActive -UnitName $ChampionUnitName -IsDryRun:$DryRun
$script:rollbackChallengerWasActive = $challengerWasActive
$script:rollbackChampionWasActive = $championWasActive
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
        $script:rollbackPreviousChampionRunId = $championRunIdAtStart
        $startedTsMs = [int64](Get-PropValue -ObjectValue $previousState -Name "started_ts_ms" -DefaultValue 0)
        $previousLaneMode = [string](Get-PropValue -ObjectValue $previousState -Name "lane_mode" -DefaultValue "")
        $previousPromotionEligible = [bool](Get-PropValue -ObjectValue $previousState -Name "promotion_eligible" -DefaultValue $true)
        if ((-not [string]::IsNullOrWhiteSpace($candidateRunId)) -and ($startedTsMs -gt 0) -and $previousPromotionEligible -and ($previousLaneMode -ne "bootstrap_latest_inclusive")) {
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
                $promotedAtTsMs = [int64](Get-Date -UFormat %s) * 1000
                $promoteExec = Invoke-CommandCapture -Exe $resolvedPythonExe -ArgList @(
                    "-m", "autobot.cli",
                    "model", "promote",
                    "--model-ref", $candidateRunId,
                    "--model-family", "train_v4_crypto_cs"
                )
                $promotionPerformed = $true
                $script:rollbackPromotionPerformed = $true
                $restartedUnits = New-Object System.Collections.Generic.List[string]
                $startedFromInactiveUnits = New-Object System.Collections.Generic.List[string]
                $skippedUnits = New-Object System.Collections.Generic.List[object]
                Restart-Unit -UnitName $ChampionUnitName
                $restartedUnits.Add($ChampionUnitName) | Out-Null
                if (-not $championWasActive) {
                    $startedFromInactiveUnits.Add($ChampionUnitName) | Out-Null
                    $script:rollbackStartedInactivePromotionUnits.Add($ChampionUnitName) | Out-Null
                }
                foreach ($unit in $resolvedPromotionTargetUnits) {
                    $trimmedUnit = [string]$unit
                    if ([string]::IsNullOrWhiteSpace($trimmedUnit)) {
                        continue
                    }
                    if ($trimmedUnit -eq $ChampionUnitName) {
                        continue
                    }
                    $targetPolicy = Resolve-PromotionTargetPolicy -Root $resolvedProjectRoot -UnitName $trimmedUnit
                    if (-not [bool](Get-PropValue -ObjectValue $targetPolicy -Name "allowed" -DefaultValue $false)) {
                        $skippedUnits.Add([ordered]@{
                            unit = $trimmedUnit
                            reason = [string](Get-PropValue -ObjectValue $targetPolicy -Name "reason" -DefaultValue "SKIPPED")
                            is_live_target = [bool](Get-PropValue -ObjectValue $targetPolicy -Name "is_live_target" -DefaultValue $false)
                        }) | Out-Null
                        continue
                    }
                    $targetWasActive = Test-SystemdUnitActive -UnitName $trimmedUnit -IsDryRun:$DryRun
                    Restart-Unit -UnitName $trimmedUnit
                    $restartedUnits.Add($trimmedUnit) | Out-Null
                    if ($targetWasActive) {
                        $script:rollbackPreviouslyActivePromotionUnits.Add($trimmedUnit) | Out-Null
                    } else {
                        $startedFromInactiveUnits.Add($trimmedUnit) | Out-Null
                        $script:rollbackStartedInactivePromotionUnits.Add($trimmedUnit) | Out-Null
                    }
                }
                $primaryLiveTargetUnit = [string](
                    $resolvedPromotionTargetUnits |
                        Where-Object { -not [string]::IsNullOrWhiteSpace([string]$_) -and ([string]$_).Trim().StartsWith("autobot-live") } |
                        Select-Object -First 1
                )
                $restartedUnitsArray = @($restartedUnits.ToArray())
                $startedFromInactiveUnitsArray = @($startedFromInactiveUnits.ToArray())
                $configuredTargetUnitsArray = @($resolvedPromotionTargetUnits)
                $skippedUnitsArray = @($skippedUnits.ToArray())
                $promoteCutover = [ordered]@{
                    batch_date = $resolvedBatchDate
                    previous_champion_run_id = $championRunIdAtStart
                    new_champion_run_id = $candidateRunId
                    promoted_at_ts_ms = $promotedAtTsMs
                    promoted_at_utc = (Get-Date).ToUniversalTime().ToString("o")
                    champion_unit = $ChampionUnitName
                    target_units = $restartedUnitsArray
                    started_from_inactive_units = $startedFromInactiveUnitsArray
                    configured_target_units = $configuredTargetUnitsArray
                    skipped_target_units = $skippedUnitsArray
                    live_rollout_contract = (Get-PropValue -ObjectValue (Load-JsonOrEmpty -PathValue (Resolve-LiveRolloutLatestPath -Root $resolvedProjectRoot -UnitName $primaryLiveTargetUnit)) -Name "contract" -DefaultValue @{})
                }
                New-Item -ItemType Directory -Force -Path $promoteCutoverArchiveRoot | Out-Null
                $promoteCutoverArchivePath = Join-Path $promoteCutoverArchiveRoot ("cutover_" + (Get-Date -Format "yyyyMMdd-HHmmss") + "_" + $candidateRunId + ".json")
                Write-JsonFile -PathValue $promoteCutoverLatestPath -Payload $promoteCutover
                Write-JsonFile -PathValue $promoteCutoverArchivePath -Payload $promoteCutover
                $script:rollbackPromoteCutoverArchivePath = $promoteCutoverArchivePath
                $report.steps.promote_previous_challenger = [ordered]@{
                    attempted = $true
                    command = $promoteExec.Command
                    output_preview = $promoteExec.Output
                    promoted = $true
                    candidate_run_id = $candidateRunId
                    restarted_units = $restartedUnitsArray
                    started_from_inactive_units = $startedFromInactiveUnitsArray
                    skipped_units = $skippedUnitsArray
                    cutover_artifact = $promoteCutoverLatestPath
                }
            } else {
                $report.steps.promote_previous_challenger = [ordered]@{
                    attempted = $false
                    promoted = $false
                    candidate_run_id = $candidateRunId
                    reason = if ($shouldPromote) { "DRY_RUN" } else { [string](Get-PropValue -ObjectValue (Get-PropValue -ObjectValue $promotionDecision -Name "decision" -DefaultValue @{}) -Name "decision" -DefaultValue "keep_champion") }
                }
            }
        } elseif ((-not [string]::IsNullOrWhiteSpace($candidateRunId)) -and ($startedTsMs -gt 0) -and ((-not $previousPromotionEligible) -or ($previousLaneMode -eq "bootstrap_latest_inclusive"))) {
            $report.steps.promote_previous_challenger = [ordered]@{
                attempted = $false
                promoted = $false
                candidate_run_id = $candidateRunId
                reason = "BOOTSTRAP_ONLY_POLICY"
                lane_mode = $previousLaneMode
                promotion_eligible = $previousPromotionEligible
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
    $executionContractRefreshAvailable = Test-Path $resolvedExecutionPolicyRefreshScript
    $executionContractGateEnforced = (-not $DryRun) -and $executionContractRefreshAvailable
    $executionContractRefresh = $null
    if ($executionContractRefreshAvailable) {
        $executionContractRefresh = Invoke-ExecutionContractRefresh `
            -PwshExe $psExe `
            -RefreshScriptPath $resolvedExecutionPolicyRefreshScript `
            -Root $resolvedProjectRoot `
            -PyExe $resolvedPythonExe `
            -IsDryRun:$DryRun
        $report.steps.refresh_execution_contract = [ordered]@{
            exit_code = [int]$executionContractRefresh.ExitCode
            command = [string]$executionContractRefresh.Command
            output_preview = [string]$executionContractRefresh.Output
            output_path = [string]$executionContractRefresh.OutputPath
            rows_total = [int]$executionContractRefresh.RowsTotal
            enforced = [bool]$executionContractGateEnforced
        }
        if ($executionContractGateEnforced -and (([int]$executionContractRefresh.ExitCode -ne 0) -or ([int]$executionContractRefresh.RowsTotal -lt [int]$ExecutionContractMinRows))) {
            throw (
                "execution contract gate failed (exit_code={0}, rows_total={1}, min_rows={2})" -f
                [int]$executionContractRefresh.ExitCode,
                [int]$executionContractRefresh.RowsTotal,
                [int]$ExecutionContractMinRows
            )
        }
    } else {
        $report.steps.refresh_execution_contract = [ordered]@{
            attempted = $false
            enforced = $false
            reason = "SCRIPT_MISSING_SKIP_NONFATAL"
            script_path = $resolvedExecutionPolicyRefreshScript
            rows_total = 0
        }
    }
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
    if ($resolvedBlockOnActiveUnits.Count -gt 0) {
        $acceptArgs += "-BlockOnActiveUnits"
        $acceptArgs += (Join-DelimitedStringArray -Values $resolvedBlockOnActiveUnits)
    }
    if ($resolvedAcceptanceArgs.Count -gt 0) {
        $acceptArgs += $resolvedAcceptanceArgs
    }

    $acceptExec = Invoke-CommandCapture -Exe $psExe -ArgList $acceptArgs -AllowFailure
    $acceptReportPath = Resolve-ReportedJsonPath -OutputText $acceptExec.Output
    $acceptReport = Load-JsonOrEmpty -PathValue $acceptReportPath
    if (Test-AcceptanceFatalFailure -ExitCode $acceptExec.ExitCode -AcceptanceReport $acceptReport) {
        throw ("candidate acceptance failed unexpectedly (exit_code={0}, report={1})" -f $acceptExec.ExitCode, $acceptReportPath)
    }
    $candidateRunId = [string](Get-PropValue -ObjectValue (Get-PropValue -ObjectValue (Get-PropValue -ObjectValue $acceptReport -Name "steps" -DefaultValue @{}) -Name "train" -DefaultValue @{}) -Name "candidate_run_id" -DefaultValue "")
    $backtestPass = [bool](Get-PropValue -ObjectValue (Get-PropValue -ObjectValue (Get-PropValue -ObjectValue $acceptReport -Name "gates" -DefaultValue @{}) -Name "backtest" -DefaultValue @{}) -Name "pass" -DefaultValue $false)
    $overallPass = [bool](Get-PropValue -ObjectValue (Get-PropValue -ObjectValue $acceptReport -Name "gates" -DefaultValue @{}) -Name "overall_pass" -DefaultValue $false)
    $acceptReasons = Get-StringArray -Value (Get-PropValue -ObjectValue $acceptReport -Name "reasons" -DefaultValue @())
    $acceptCandidate = Get-PropValue -ObjectValue $acceptReport -Name "candidate" -DefaultValue @{}
    $acceptSplitPolicy = Get-PropValue -ObjectValue $acceptReport -Name "split_policy" -DefaultValue @{}
    $acceptLaneMode = [string](Get-PropValue -ObjectValue $acceptCandidate -Name "lane_mode" -DefaultValue "")
    if ([string]::IsNullOrWhiteSpace($acceptLaneMode)) {
        $acceptLaneMode = [string](Get-PropValue -ObjectValue $acceptSplitPolicy -Name "lane_mode" -DefaultValue "")
    }
    $acceptPromotionEligible = [bool](Get-PropValue -ObjectValue $acceptCandidate -Name "promotion_eligible" -DefaultValue $true)
    $bootstrapOnly = Test-BootstrapOnlyAcceptanceReport -AcceptanceReport $acceptReport
    $scoutOnlyBudgetEvidence = Test-ScoutOnlyBudgetEvidence -AcceptanceReport $acceptReport
    $executionContractOutputPath = ""
    $executionContractRowsTotal = 0
    if ($null -ne $executionContractRefresh) {
        $executionContractOutputPath = [string]$executionContractRefresh.OutputPath
        $executionContractRowsTotal = [int]$executionContractRefresh.RowsTotal
    }
    $report.steps.train_candidate = [ordered]@{
        exit_code = [int]$acceptExec.ExitCode
        command = $acceptExec.Command
        output_preview = $acceptExec.Output
        report_path = $acceptReportPath
        execution_contract_output_path = $executionContractOutputPath
        execution_contract_rows_total = $executionContractRowsTotal
        candidate_run_id = $candidateRunId
        backtest_pass = $backtestPass
        overall_pass = $overallPass
        lane_mode = $acceptLaneMode
        promotion_eligible = $acceptPromotionEligible
        bootstrap_only = $bootstrapOnly
        reasons = @($acceptReasons)
    }

    if ((-not [string]::IsNullOrWhiteSpace($candidateRunId)) -and ($backtestPass -or $bootstrapOnly)) {
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
            lane_mode = $acceptLaneMode
            promotion_eligible = $acceptPromotionEligible
            bootstrap_only = $bootstrapOnly
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
            promotion_target_units = @($resolvedPromotionTargetUnits)
            lane_mode = $acceptLaneMode
            promotion_eligible = $acceptPromotionEligible
            bootstrap_only = $bootstrapOnly
            split_policy_id = [string](Get-PropValue -ObjectValue $acceptSplitPolicy -Name "policy_id" -DefaultValue "")
            split_policy_artifact_path = [string](Get-PropValue -ObjectValue $acceptCandidate -Name "split_policy_artifact_path" -DefaultValue "")
        }
        if (-not $DryRun) {
            Write-JsonFile -PathValue $statePath -Payload $nextState
        }
        $report.challenger_next = $nextState

        if ($resolvedCandidateTargetUnits.Count -gt 0) {
            $restartedCandidateUnits = @()
            $skippedCandidateUnits = @()
            foreach ($unit in $resolvedCandidateTargetUnits) {
                $trimmedUnit = [string]$unit
                if ([string]::IsNullOrWhiteSpace($trimmedUnit)) {
                    continue
                }
                $trimmedUnit = $trimmedUnit.Trim()
                if (Test-SystemdUnitActive -UnitName $trimmedUnit -IsDryRun:$DryRun) {
                    Restart-Unit -UnitName $trimmedUnit
                    $restartedCandidateUnits += $trimmedUnit
                } else {
                    $skippedCandidateUnits += [ordered]@{
                        unit = $trimmedUnit
                        reason = "UNIT_NOT_ACTIVE"
                    }
                }
            }
            $report.steps.restart_candidate_targets = [ordered]@{
                attempted = $true
                candidate_run_id = $candidateRunId
                restarted_units = @($restartedCandidateUnits)
                skipped_units = @($skippedCandidateUnits)
            }
        } else {
            $report.steps.restart_candidate_targets = [ordered]@{
                attempted = $false
                reason = "NO_CANDIDATE_TARGET_UNITS"
                candidate_run_id = $candidateRunId
            }
        }
    } else {
        $report.steps.start_challenger = [ordered]@{
            skipped = $true
            candidate_run_id = $candidateRunId
            acceptance_exit_code = [int]$acceptExec.ExitCode
            acceptance_reasons = @($acceptReasons)
            acceptance_notes = @((Get-PropValue -ObjectValue $acceptReport -Name "notes" -DefaultValue @()))
            reason = if ([string]::IsNullOrWhiteSpace($candidateRunId)) {
                "NO_CANDIDATE_RUN_ID"
            } elseif ($acceptReasons -contains "DUPLICATE_CANDIDATE") {
                "DUPLICATE_CANDIDATE"
            } elseif ($acceptReasons -contains "TRAINER_EVIDENCE_REQUIRED_FAILED") {
                "TRAINER_EVIDENCE_REQUIRED_FAILED"
            } elseif ($scoutOnlyBudgetEvidence) {
                "SCOUT_ONLY_BUDGET_EVIDENCE"
            } elseif ($bootstrapOnly) {
                "BOOTSTRAP_ONLY_POLICY"
            } elseif (-not $backtestPass) {
                "BACKTEST_SANITY_FAILED"
            } elseif (-not $overallPass) {
                "ACCEPTANCE_REJECTED"
            } else {
                "UNKNOWN"
            }
        }
        $report.steps.restart_candidate_targets = [ordered]@{
            attempted = $false
            candidate_run_id = $candidateRunId
            reason = [string]$report.steps.start_challenger.reason
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
    $report.steps.restart_candidate_targets = [ordered]@{
        attempted = $false
        reason = "SKIPPED_BY_MODE"
    }
}

$report.steps.rollback = [ordered]@{
    attempted = $false
    reason = "NOT_REQUIRED"
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
exit $exitCode
