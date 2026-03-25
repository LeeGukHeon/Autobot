param(
    [string]$ProjectRoot = "",
    [string]$PythonExe = "",
    [string]$AcceptanceScript = "",
    [string]$PairedPaperScript = "",
    [string]$RuntimeInstallScript = "",
    [string]$CandidateAdoptionScript = "",
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
    [int]$PairedPaperDurationSec = 360,
    [int]$PairedPaperMinMatchedOpportunities = 1,
    [string]$PairedPaperQuote = "KRW",
    [int]$PairedPaperTopN = 20,
    [string]$PairedPaperTf = "5m",
    [string]$PairedPaperPreset = "live_v4",
    [string]$PairedPaperModelFamily = "train_v4_crypto_cs",
    [string]$PairedPaperFeatureSet = "v4",
    [string]$PairedPaperFeatureProvider = "live_v4",
    [string]$PairedPaperMicroProvider = "live_ws",
    [int]$PairedPaperWarmupSec = 60,
    [int]$PairedPaperWarmupMinTradeEventsPerMarket = 1,
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
. (Join-Path $PSScriptRoot "v4_candidate_state_helpers.ps1")

function Resolve-DefaultAcceptanceScript {
    param([string]$Root)
    return (Join-Path $Root "scripts/v4_governed_candidate_acceptance.ps1")
}

function Resolve-DefaultPairedPaperScript {
    param([string]$Root)
    return (Join-Path $Root "scripts/paired_paper_soak.ps1")
}

function Resolve-DefaultRuntimeInstallScript {
    param([string]$Root)
    return (Join-Path $Root "scripts/install_server_runtime_services.ps1")
}

function Resolve-DefaultCandidateAdoptionScript {
    param([string]$Root)
    return (Join-Path $PSScriptRoot "adopt_v4_candidate_for_server.ps1")
}

function Resolve-DefaultExecutionPolicyRefreshScript {
    param([string]$Root)
    return (Join-Path $Root "scripts/refresh_live_execution_policy.ps1")
}

function Resolve-ChampionRunId {
    param([string]$Root)
    return (Resolve-V4ChampionRunId -Root $Root)
}

function Clear-LatestCandidatePointers {
    param(
        [string]$RegistryRoot,
        [string]$Family
    )
    return (Clear-V4LatestCandidatePointers -RegistryRoot $RegistryRoot -Family $Family -DryRun:$DryRun)
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

function Invoke-PreflightCapture {
    param(
        [string]$PwshExe,
        [string]$PreflightScriptPath,
        [string]$Root,
        [string]$PythonPath,
        [string]$ChampionUnit,
        [string]$ChallengerUnit,
        [string[]]$PromotionUnits,
        [string[]]$CandidateUnits
    )
    $args = @(
        "-NoProfile",
        "-ExecutionPolicy", "Bypass",
        "-File", $PreflightScriptPath,
        "-ProjectRoot", $Root,
        "-PythonExe", $PythonPath,
        "-ModelFamily", "train_v4_crypto_cs",
        "-RequiredPointers", "champion",
        "-CheckCandidateStateConsistency",
        "-FailOnDirtyWorktree"
    )
    if ([System.IO.Path]::DirectorySeparatorChar -ne '\') {
        $requiredUnits = @()
        foreach ($value in @($ChampionUnit, $ChallengerUnit) + @($PromotionUnits) + @($CandidateUnits)) {
            $text = [string]$value
            if ([string]::IsNullOrWhiteSpace($text)) {
                continue
            }
            $trimmed = $text.Trim()
            if ($requiredUnits -contains $trimmed) {
                continue
            }
            $requiredUnits += $trimmed
        }
        foreach ($timerUnit in @("autobot-v4-challenger-spawn.timer", "autobot-v4-challenger-promote.timer")) {
            if ($requiredUnits -contains $timerUnit) {
                continue
            }
            $requiredUnits += $timerUnit
        }
        if ($requiredUnits.Count -gt 0) {
            $serializedUnits = Join-DelimitedStringArray -Values $requiredUnits
            $failedUnitsList = @($requiredUnits)
            foreach ($value in @("autobot-v4-challenger-spawn.service", "autobot-v4-challenger-promote.service")) {
                if ($failedUnitsList -contains $value) {
                    continue
                }
                $failedUnitsList += $value
            }
            $failedUnits = Join-DelimitedStringArray -Values $failedUnitsList
            $expectedUnitStates = @(
                ($ChampionUnit + "=enabled"),
                ($ChallengerUnit + "=disabled"),
                "autobot-v4-challenger-spawn.timer=enabled",
                "autobot-v4-challenger-promote.timer=enabled",
                "autobot-paper-v4-replay.service=disabled",
                "autobot-live-alpha-replay-shadow.service=disabled"
            )
            foreach ($value in @($PromotionUnits) + @($CandidateUnits)) {
                $text = [string]$value
                if ([string]::IsNullOrWhiteSpace($text)) {
                    continue
                }
                $expectedUnitStates += ($text.Trim() + "=enabled")
            }
            $requiredStateDbPaths = @(
                "data/state/live_candidate/live_state.db",
                "data/state/live_state.db"
            )
            $args += @(
                "-RequiredUnitFiles",
                $serializedUnits,
                "-BlockOnFailedUnits",
                $failedUnits,
                "-ExpectedUnitStates",
                (Join-DelimitedStringArray -Values $expectedUnitStates),
                "-RequiredStateDbPaths",
                (Join-DelimitedStringArray -Values $requiredStateDbPaths)
            )
        }
    }
    $output = & $PwshExe @args 2>&1
    $exitCode = [int]$LASTEXITCODE
    return [PSCustomObject]@{
        ExitCode = $exitCode
        Output = ($output -join "`n")
        Command = ($PwshExe + " " + (($args | ForEach-Object { Quote-ShellArg ([string]$_) }) -join " "))
        ReportPath = (Join-Path $Root "logs/ops/server_preflight/latest.json")
    }
}

function Resolve-ReportedJsonPath {
    param([string]$OutputText)
    if ([string]::IsNullOrWhiteSpace($OutputText)) {
        return ""
    }
    $regex = [System.Text.RegularExpressions.Regex]::new("(?m)^\[[^\]]+\]\s+report=(.+)$")
    $matches = $regex.Matches([string]$OutputText)
    if ($null -eq $matches -or $matches.Count -eq 0) {
        return ""
    }
    for ($index = $matches.Count - 1; $index -ge 0; $index--) {
        $candidatePath = [string]$matches[$index].Groups[1].Value.Trim()
        if ([string]::IsNullOrWhiteSpace($candidatePath)) {
            continue
        }
        if ([string]::Equals([System.IO.Path]::GetExtension($candidatePath), ".json", [System.StringComparison]::OrdinalIgnoreCase)) {
            return $candidatePath
        }
    }
    return [string]$matches[$matches.Count - 1].Groups[1].Value.Trim()
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
    $propertyNames = @(
        $ObjectValue.PSObject.Properties |
            Where-Object { $null -ne $_ -and $null -ne $_.Name } |
            ForEach-Object { [string]$_.Name }
    )
    if ($propertyNames -contains $Name) {
        return $ObjectValue.$Name
    }
    return $DefaultValue
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

function Get-StringArray {
    param([Parameter(Mandatory = $false)]$Value)
    return @(Expand-DelimitedStringArray -Value $Value)
}

function Resolve-ExecutionContractRowsTotal {
    param([Parameter(Mandatory = $false)]$Payload)
    $executionContract = Get-PropValue -ObjectValue $Payload -Name "execution_contract" -DefaultValue @{}
    $rows = [int](Get-PropValue -ObjectValue $executionContract -Name "rows_total" -DefaultValue 0)
    if ($rows -gt 0) {
        return $rows
    }
    $rows = [int](Get-PropValue -ObjectValue $Payload -Name "rows_total" -DefaultValue 0)
    if ($rows -gt 0) {
        return $rows
    }
    $outputPath = [string](Get-PropValue -ObjectValue $Payload -Name "output_path" -DefaultValue "")
    if (-not [string]::IsNullOrWhiteSpace($outputPath) -and (Test-Path $outputPath)) {
        $outputDoc = Load-JsonOrEmpty -PathValue $outputPath
        $rows = [int](Get-PropValue -ObjectValue $outputDoc -Name "rows_total" -DefaultValue 0)
        if ($rows -gt 0) {
            return $rows
        }
        $nestedExecutionContract = Get-PropValue -ObjectValue $outputDoc -Name "execution_contract" -DefaultValue @{}
        $rows = [int](Get-PropValue -ObjectValue $nestedExecutionContract -Name "rows_total" -DefaultValue 0)
        if ($rows -gt 0) {
            return $rows
        }
    }
    return 0
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

function Test-BootstrapOnlyAcceptanceReport {
    param([Parameter(Mandatory = $false)]$AcceptanceReport)
    if (-not (Test-ObjectHasValues -ObjectValue $AcceptanceReport)) {
        return $false
    }
    $reasons = Get-StringArray -Value (Get-PropValue -ObjectValue $AcceptanceReport -Name "reasons" -DefaultValue @())
    if ($reasons -contains "BOOTSTRAP_ONLY_POLICY") {
        return $true
    }
    $candidate = Get-PropValue -ObjectValue $AcceptanceReport -Name "candidate" -DefaultValue @{}
    $splitPolicy = Get-PropValue -ObjectValue $AcceptanceReport -Name "split_policy" -DefaultValue @{}
    $laneMode = [string](Get-PropValue -ObjectValue $candidate -Name "lane_mode" -DefaultValue "")
    if ([string]::IsNullOrWhiteSpace($laneMode)) {
        $laneMode = [string](Get-PropValue -ObjectValue $splitPolicy -Name "lane_mode" -DefaultValue "")
    }
    $promotionEligible = [bool](Get-PropValue -ObjectValue $candidate -Name "promotion_eligible" -DefaultValue $true)
    if (($laneMode -eq "bootstrap_latest_inclusive") -and (-not $promotionEligible)) {
        return $true
    }
    return $false
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

function Stop-ConfiguredUnits {
    param([string[]]$Units)
    $stoppedUnits = New-Object System.Collections.Generic.List[string]
    $skippedUnits = New-Object System.Collections.Generic.List[object]
    foreach ($unit in @($Units)) {
        $trimmedUnit = [string]$unit
        if ([string]::IsNullOrWhiteSpace($trimmedUnit)) {
            continue
        }
        $trimmedUnit = $trimmedUnit.Trim()
        if (Test-SystemdUnitActive -UnitName $trimmedUnit) {
            Stop-UnitIfActive -UnitName $trimmedUnit | Out-Null
            $stoppedUnits.Add($trimmedUnit) | Out-Null
        } else {
            $skippedUnits.Add([ordered]@{
                unit = $trimmedUnit
                reason = "UNIT_NOT_ACTIVE"
            }) | Out-Null
        }
    }
    return [ordered]@{
        attempted = $true
        stopped_units = @($stoppedUnits.ToArray())
        skipped_units = @($skippedUnits.ToArray())
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
$resolvedPairedPaperScript = if ([string]::IsNullOrWhiteSpace($PairedPaperScript)) { Resolve-DefaultPairedPaperScript -Root $resolvedProjectRoot } else { $PairedPaperScript }
$resolvedRuntimeInstallScript = if ([string]::IsNullOrWhiteSpace($RuntimeInstallScript)) { Resolve-DefaultRuntimeInstallScript -Root $resolvedProjectRoot } else { $RuntimeInstallScript }
$resolvedCandidateAdoptionScript = if ([string]::IsNullOrWhiteSpace($CandidateAdoptionScript)) { Resolve-DefaultCandidateAdoptionScript -Root $resolvedProjectRoot } else { $CandidateAdoptionScript }
$resolvedExecutionPolicyRefreshScript = if ([string]::IsNullOrWhiteSpace($ExecutionPolicyRefreshScript)) { Resolve-DefaultExecutionPolicyRefreshScript -Root $resolvedProjectRoot } else { $ExecutionPolicyRefreshScript }
$resolvedBatchDate = Resolve-BatchDateValue -DateText $BatchDate
$resolvedPromotionTargetUnits = @(Get-StringArray -Value $PromotionTargetUnits)
$resolvedCandidateTargetUnits = @(Get-StringArray -Value $CandidateTargetUnits)
$resolvedBlockOnActiveUnits = @(Get-StringArray -Value $BlockOnActiveUnits)
$resolvedAcceptanceArgs = @(Get-StringArray -Value $AcceptanceArgs)
$registryRoot = Join-Path $resolvedProjectRoot "models/registry"
$stateRoot = Join-Path $resolvedProjectRoot "logs/model_v4_challenger"
$statePath = Join-Path $stateRoot "current_state.json"
$archiveRoot = Join-Path $stateRoot "archive"
$pairedPaperRoot = Join-Path $resolvedProjectRoot "logs/paired_paper"
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
    challenger_previous_paired = @{}
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

$preflightScriptPath = Join-Path $PSScriptRoot "check_server_preflight.ps1"
$preflightExec = Invoke-PreflightCapture `
    -PwshExe $psExe `
    -PreflightScriptPath $preflightScriptPath `
    -Root $resolvedProjectRoot `
    -PythonPath $resolvedPythonExe `
    -ChampionUnit $ChampionUnitName `
    -ChallengerUnit $ChallengerUnitName `
    -PromotionUnits $resolvedPromotionTargetUnits `
    -CandidateUnits $resolvedCandidateTargetUnits
$preflightReport = Load-JsonOrEmpty -PathValue $preflightExec.ReportPath
$report.steps.preflight = [ordered]@{
    attempted = $true
    exit_code = [int]$preflightExec.ExitCode
    command = $preflightExec.Command
    output_preview = [string]$preflightExec.Output
    report_path = $preflightExec.ReportPath
    summary = Get-PropValue -ObjectValue $preflightReport -Name "summary" -DefaultValue @{}
}
if ($preflightExec.ExitCode -ne 0) {
    $exitCode = 2
    $report.exception = [ordered]@{
        message = "server preflight failed"
    }
    $report.completed_at_utc = (Get-Date).ToUniversalTime().ToString("o")
    if (-not $DryRun) {
        Write-JsonFile -PathValue $reportPath -Payload $report
        Write-JsonFile -PathValue $latestReportPath -Payload $report
    }
    Write-Host ("[daily-cc][error] mode={0} reason=SERVER_PREFLIGHT_FAILED" -f $Mode)
    Write-Host ("[daily-cc] batch_date={0}" -f $resolvedBatchDate)
    Write-Host ("[daily-cc] report={0}" -f $reportPath)
    Write-Host ("[daily-cc] latest={0}" -f $latestReportPath)
    Write-Host ("[daily-cc] preflight_report={0}" -f $preflightExec.ReportPath)
    exit $exitCode
}

$previousState = Load-JsonOrEmpty -PathValue $statePath
$hasPreviousState = Test-ObjectHasValues -ObjectValue $previousState
$challengerWasActive = Test-SystemdUnitActive -UnitName $ChallengerUnitName
$championWasActive = Test-SystemdUnitActive -UnitName $ChampionUnitName
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
            $pairedPaperArgs = @(
                "-NoProfile",
                "-ExecutionPolicy", "Bypass",
                "-File", $resolvedPairedPaperScript,
                "-ProjectRoot", $resolvedProjectRoot,
                "-PythonExe", $resolvedPythonExe,
                "-DurationSec", [string]$PairedPaperDurationSec,
                "-Quote", $PairedPaperQuote,
                "-TopN", [string]$PairedPaperTopN,
                "-Tf", $PairedPaperTf,
                "-ChampionModelRef", $championRunIdAtStart,
                "-ChallengerModelRef", $candidateRunId,
                "-ModelFamily", $PairedPaperModelFamily,
                "-FeatureSet", $PairedPaperFeatureSet,
                "-Preset", $PairedPaperPreset,
                "-PaperMicroProvider", $PairedPaperMicroProvider,
                "-PaperFeatureProvider", $PairedPaperFeatureProvider,
                "-WarmupSec", [string]$PairedPaperWarmupSec,
                "-WarmupMinTradeEventsPerMarket", [string]$PairedPaperWarmupMinTradeEventsPerMarket,
                "-MinMatchedOpportunities", [string]$PairedPaperMinMatchedOpportunities,
                "-OutDir", $pairedPaperRoot
            )
            $pairedPaperExec = Invoke-CommandCapture -Exe $psExe -ArgList $pairedPaperArgs -AllowFailure
            $pairedPaperReportPath = Resolve-ReportedJsonPath -OutputText ([string]$pairedPaperExec.Output)
            $pairedPaperArtifact = if ((-not [string]::IsNullOrWhiteSpace($pairedPaperReportPath)) -and (Test-Path $pairedPaperReportPath)) {
                Load-JsonOrEmpty -PathValue $pairedPaperReportPath
            } else {
                @{}
            }
            $pairedPaperGate = Get-PropValue -ObjectValue $pairedPaperArtifact -Name "gate" -DefaultValue @{}
            $report.steps.paired_paper_previous_challenger = [ordered]@{
                attempted = $true
                exit_code = [int]$pairedPaperExec.ExitCode
                command = $pairedPaperExec.Command
                output_preview = [string]$pairedPaperExec.Output
                report_path = [string]$pairedPaperReportPath
                gate = $pairedPaperGate
            }
            $report.challenger_previous_paired = $pairedPaperArtifact
            if (($pairedPaperExec.ExitCode -ne 0) -or (-not (Test-ObjectHasValues -ObjectValue $pairedPaperArtifact))) {
                $promotionDecision = [ordered]@{
                    decision = [ordered]@{
                        promote = $false
                        decision = "keep_champion"
                    }
                    paired_paper = [ordered]@{
                        evaluated = $false
                        pass = $false
                        reason = "PAIRED_PAPER_EXECUTION_FAILED"
                        report_path = [string]$pairedPaperReportPath
                    }
                }
                $report.challenger_previous = $promotionDecision
                $report.steps.stop_candidate_targets_after_promote = [ordered]@{
                    attempted = $false
                    reason = "PAIRED_PAPER_EXECUTION_FAILED"
                    candidate_run_id = $candidateRunId
                }
                $report.steps.clear_latest_candidate = [ordered]@{
                    attempted = $false
                    reason = "PAIRED_PAPER_EXECUTION_FAILED"
                    candidate_run_id = $candidateRunId
                }
                $report.steps.promote_previous_challenger = [ordered]@{
                    attempted = $false
                    promoted = $false
                    candidate_run_id = $candidateRunId
                    reason = "PAIRED_PAPER_EXECUTION_FAILED"
                }
            } else {
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
            $promotionDecision | Add-Member -NotePropertyName "paired_paper" -NotePropertyValue $pairedPaperArtifact -Force
            $report.challenger_previous = $promotionDecision
            if (-not $DryRun) {
                New-Item -ItemType Directory -Force -Path $archiveRoot | Out-Null
                $archivePath = Join-Path $archiveRoot ("challenger_" + (Get-Date -Format "yyyyMMdd-HHmmss") + "_" + $candidateRunId + ".json")
                Write-JsonFile -PathValue $archivePath -Payload ([ordered]@{
                    state = $previousState
                    paired_paper = $pairedPaperArtifact
                    comparison = $promotionDecision
                })
            }
            $shouldPromote = [bool](Get-PropValue -ObjectValue (Get-PropValue -ObjectValue $promotionDecision -Name "decision" -DefaultValue @{}) -Name "promote" -DefaultValue $false)
            $pairedPaperPass = [bool](Get-PropValue -ObjectValue $pairedPaperGate -Name "pass" -DefaultValue $false)
            if (-not $pairedPaperPass) {
                $shouldPromote = $false
            }
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
                    $targetWasActive = Test-SystemdUnitActive -UnitName $trimmedUnit
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
                $candidateTargetStopStep = Stop-ConfiguredUnits -Units $resolvedCandidateTargetUnits
                $clearCandidatePointerStep = Clear-LatestCandidatePointers -RegistryRoot $registryRoot -Family "train_v4_crypto_cs"
                $report.steps.stop_candidate_targets_after_promote = $candidateTargetStopStep
                $report.steps.clear_latest_candidate = $clearCandidatePointerStep
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
                $report.steps.stop_candidate_targets_after_promote = [ordered]@{
                    attempted = $false
                    reason = if ($shouldPromote) { "DRY_RUN" } else { "PROMOTION_NOT_PERFORMED" }
                }
                $report.steps.clear_latest_candidate = [ordered]@{
                    attempted = $false
                    reason = if ($shouldPromote) { "DRY_RUN" } else { "PROMOTION_NOT_PERFORMED" }
                }
                $report.steps.promote_previous_challenger = [ordered]@{
                    attempted = $false
                    promoted = $false
                    candidate_run_id = $candidateRunId
                    reason = if ($shouldPromote) {
                        "DRY_RUN"
                    } elseif (-not $pairedPaperPass) {
                        [string](Get-PropValue -ObjectValue $pairedPaperGate -Name "reason" -DefaultValue "PAIRED_PAPER_NOT_READY")
                    } else {
                        [string](Get-PropValue -ObjectValue (Get-PropValue -ObjectValue $promotionDecision -Name "decision" -DefaultValue @{}) -Name "decision" -DefaultValue "keep_champion")
                    }
                }
            }
            }
        } elseif ((-not [string]::IsNullOrWhiteSpace($candidateRunId)) -and ($startedTsMs -gt 0) -and ((-not $previousPromotionEligible) -or ($previousLaneMode -eq "bootstrap_latest_inclusive"))) {
            $report.steps.stop_candidate_targets_after_promote = [ordered]@{
                attempted = $false
                reason = "BOOTSTRAP_ONLY_POLICY"
                candidate_run_id = $candidateRunId
            }
            $report.steps.clear_latest_candidate = [ordered]@{
                attempted = $false
                reason = "BOOTSTRAP_ONLY_POLICY"
                candidate_run_id = $candidateRunId
            }
            $report.steps.promote_previous_challenger = [ordered]@{
                attempted = $false
                promoted = $false
                candidate_run_id = $candidateRunId
                reason = "BOOTSTRAP_ONLY_POLICY"
                lane_mode = $previousLaneMode
                promotion_eligible = $previousPromotionEligible
            }
        } else {
            $report.steps.stop_candidate_targets_after_promote = [ordered]@{
                attempted = $false
                reason = "PREVIOUS_STATE_INCOMPLETE"
                candidate_run_id = $candidateRunId
            }
            $report.steps.clear_latest_candidate = [ordered]@{
                attempted = $false
                reason = "PREVIOUS_STATE_INCOMPLETE"
                candidate_run_id = $candidateRunId
            }
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
        $report.steps.stop_candidate_targets_after_promote = [ordered]@{
            attempted = $false
            reason = "NO_PREVIOUS_CHALLENGER_STATE"
        }
        $report.steps.clear_latest_candidate = [ordered]@{
            attempted = $false
            reason = "NO_PREVIOUS_CHALLENGER_STATE"
        }
        $report.steps.promote_previous_challenger = [ordered]@{
            attempted = $false
            promoted = $false
            reason = "NO_PREVIOUS_CHALLENGER_STATE"
        }
    }
} else {
    $report.steps.stop_candidate_targets_after_promote = [ordered]@{
        attempted = $false
        reason = "SKIPPED_BY_MODE"
    }
    $report.steps.clear_latest_candidate = [ordered]@{
        attempted = $false
        reason = "SKIPPED_BY_MODE"
    }
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
        if ($overallPass) {
            $adoptArgs = @(
                "-NoProfile",
                "-ExecutionPolicy", "Bypass",
                "-File", $resolvedCandidateAdoptionScript,
                "-ProjectRoot", $resolvedProjectRoot,
                "-PythonExe", $resolvedPythonExe,
                "-RuntimeInstallScript", $resolvedRuntimeInstallScript,
                "-BatchDate", $resolvedBatchDate,
                "-CandidateRunId", $candidateRunId,
                "-ChampionUnitName", $ChampionUnitName,
                "-ChallengerUnitName", $ChallengerUnitName,
                "-LaneMode", $acceptLaneMode
            )
            if ($resolvedPromotionTargetUnits.Count -gt 0) {
                $adoptArgs += @(
                    "-PromotionTargetUnits",
                    (Join-DelimitedStringArray -Values $resolvedPromotionTargetUnits)
                )
            }
            if ($resolvedCandidateTargetUnits.Count -gt 0) {
                $adoptArgs += @(
                    "-CandidateTargetUnits",
                    (Join-DelimitedStringArray -Values $resolvedCandidateTargetUnits)
                )
            }
            $splitPolicyIdValue = [string](Get-PropValue -ObjectValue $acceptSplitPolicy -Name "policy_id" -DefaultValue "")
            if (-not [string]::IsNullOrWhiteSpace($splitPolicyIdValue)) {
                $adoptArgs += @(
                    "-SplitPolicyId",
                    $splitPolicyIdValue
                )
            }
            $splitPolicyArtifactPathValue = [string](Get-PropValue -ObjectValue $acceptCandidate -Name "split_policy_artifact_path" -DefaultValue "")
            if (-not [string]::IsNullOrWhiteSpace($splitPolicyArtifactPathValue)) {
                $adoptArgs += @(
                    "-SplitPolicyArtifactPath",
                    $splitPolicyArtifactPathValue
                )
            }
            if ($bootstrapOnly) {
                $adoptArgs += "-BootstrapOnly"
            }
            if ($DryRun) {
                $adoptArgs += "-DryRun"
            }
            $adoptExec = Invoke-CommandCapture -Exe $psExe -ArgList $adoptArgs
            $adoptReportPath = Resolve-ReportedJsonPath -OutputText $adoptExec.Output
            $adoptReport = Load-JsonOrEmpty -PathValue $adoptReportPath
            if ((-not $DryRun) -and (-not (Test-ObjectHasValues -ObjectValue $adoptReport))) {
                throw ("candidate adoption report missing: " + $adoptReportPath)
            }
            $report.steps.adopt_candidate = [ordered]@{
                attempted = $true
                command = $adoptExec.Command
                output_preview = $adoptExec.Output
                report_path = $adoptReportPath
                candidate_run_id = $candidateRunId
            }
            $report.steps.update_latest_candidate = Get-PropValue -ObjectValue (Get-PropValue -ObjectValue $adoptReport -Name "steps" -DefaultValue @{}) -Name "update_latest_candidate" -DefaultValue @{}
            $report.steps.start_challenger = Get-PropValue -ObjectValue (Get-PropValue -ObjectValue $adoptReport -Name "steps" -DefaultValue @{}) -Name "start_challenger" -DefaultValue @{}
            $report.steps.restart_candidate_targets = Get-PropValue -ObjectValue (Get-PropValue -ObjectValue $adoptReport -Name "steps" -DefaultValue @{}) -Name "restart_candidate_targets" -DefaultValue @{}
            $report.challenger_next = Get-PropValue -ObjectValue $adoptReport -Name "current_state" -DefaultValue @{}
        } else {
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
                $report.steps.restart_candidate_targets = [ordered]@{
                    attempted = $false
                    candidate_run_id = $candidateRunId
                    reason = "OVERALL_PASS_REQUIRED"
                    overall_pass = $overallPass
                }
            } else {
                $report.steps.restart_candidate_targets = [ordered]@{
                    attempted = $false
                    reason = "NO_CANDIDATE_TARGET_UNITS"
                    candidate_run_id = $candidateRunId
                }
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
            } elseif ($acceptReasons -contains "SCOUT_ONLY_BUDGET_EVIDENCE") {
                "SCOUT_ONLY_BUDGET_EVIDENCE"
            } elseif ($acceptReasons -contains "EXECUTION_POLICY_VETO_FAILURE") {
                "EXECUTION_POLICY_VETO_FAILURE"
            } elseif ($acceptReasons -contains "RUNTIME_PARITY_EXECUTION_POLICY_VETO_FAILURE") {
                "RUNTIME_PARITY_EXECUTION_POLICY_VETO_FAILURE"
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
