param(
    [string]$ProjectRoot = "",
    [string]$PythonExe = "",
    [string]$BatchDate = "",
    [string]$SummaryPath = "data/collect/_meta/train_snapshot_close_latest.json",
    [string]$RefreshSummaryPath = "data/collect/_meta/train_snapshot_training_critical_refresh_latest.json",
    [string]$FeatureRefreshSummaryPath = "data/features/features_v4/_meta/nightly_train_snapshot_contract_refresh.json",
    [string]$DataPlatformRefreshScript = "",
    [string]$FeatureContractRefreshScript = "",
    [string]$CandlesSummaryPath = "data/collect/_meta/candles_api_refresh_latest.json",
    [string]$TicksSummaryPath = "data/raw_ticks/upbit/_meta/ticks_daily_latest.json",
    [string]$MicroRoot = "data/parquet/micro_v1",
    [string]$Tf = "5m",
    [int]$MaxCandlesSummaryAgeMinutes = 120,
    [int]$MaxTicksSummaryAgeMinutes = 120,
    [switch]$SkipDeadline,
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

function Resolve-DateTimeOffsetOrNull {
    param([Parameter(Mandatory = $false)]$Value)
    if ($null -eq $Value) {
        return $null
    }
    if ($Value -is [DateTimeOffset]) {
        return ([DateTimeOffset]$Value).ToUniversalTime()
    }
    if ($Value -is [DateTime]) {
        $dateTimeValue = [DateTime]$Value
        if ($dateTimeValue.Kind -eq [System.DateTimeKind]::Unspecified) {
            return [DateTimeOffset]::new(
                [DateTime]::SpecifyKind($dateTimeValue, [System.DateTimeKind]::Utc)
            ).ToUniversalTime()
        }
        return [DateTimeOffset]$dateTimeValue.ToUniversalTime()
    }
    $textValue = [string]$Value
    if ([string]::IsNullOrWhiteSpace($textValue)) {
        return $null
    }
    try {
        return [DateTimeOffset]::Parse(
            $textValue,
            [System.Globalization.CultureInfo]::InvariantCulture,
            [System.Globalization.DateTimeStyles]::AssumeUniversal -bor [System.Globalization.DateTimeStyles]::AdjustToUniversal
        )
    } catch {
        return $null
    }
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
    Write-Host ("[train-snapshot-close] command={0}" -f $commandText)
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

function Test-StepResultsPass {
    param([Parameter(Mandatory = $false)]$Steps)
    foreach ($step in @($Steps)) {
        if ([int](Get-PropValue -ObjectValue $step -Name "exit_code" -DefaultValue 1) -ne 0) {
            return $false
        }
    }
    return $true
}

function Get-MicroDateCoverageCounts {
    param(
        [string]$MicroRoot,
        [string]$TfValue
    )
    $counts = @{}
    if ([string]::IsNullOrWhiteSpace($MicroRoot) -or (-not (Test-Path $MicroRoot))) {
        return $counts
    }
    $tfRoot = Join-Path $MicroRoot ("tf=" + $TfValue)
    if (-not (Test-Path $tfRoot)) {
        return $counts
    }
    $marketDirs = @(Get-ChildItem -Path $tfRoot -Directory -Filter "market=*")
    foreach ($marketDir in $marketDirs) {
        foreach ($dateDir in @(Get-ChildItem -Path $marketDir.FullName -Directory -Filter "date=*")) {
            if ($dateDir.Name -notmatch "^date=(\d{4}-\d{2}-\d{2})$") {
                continue
            }
            $dateValue = [string]$Matches[1]
            if (-not $counts.ContainsKey($dateValue)) {
                $counts[$dateValue] = 0
            }
            $counts[$dateValue] = [int]$counts[$dateValue] + 1
        }
    }
    return $counts
}

function Get-SourceFreshnessResult {
    param(
        [string]$Name,
        [string]$SummaryFile,
        [int]$MaxAgeMinutes,
        [string]$BatchDateValue = ""
    )
    $payload = Load-JsonOrEmpty -PathValue $SummaryFile
    $exists = (Test-Path $SummaryFile) -and ($payload -ne $null)
    $generatedAtRaw = Get-PropValue -ObjectValue $payload -Name "generated_at_utc" -DefaultValue $null
    if ($null -eq $generatedAtRaw -or [string]::IsNullOrWhiteSpace([string]$generatedAtRaw)) {
        $generatedAtRaw = Get-PropValue -ObjectValue $payload -Name "generated_at" -DefaultValue $null
    }
    $generatedAt = Resolve-DateTimeOffsetOrNull -Value $generatedAtRaw
    $generatedAtUtc = if ($null -eq $generatedAt) {
        [string]$generatedAtRaw
    } else {
        $generatedAt.ToString("o")
    }
    $ageMinutes = $null
    if ($null -ne $generatedAt) {
        $ageMinutes = [Math]::Round(([DateTimeOffset]::UtcNow - $generatedAt.ToUniversalTime()).TotalMinutes, 2)
    }
    $steps = @(
        @(Get-PropValue -ObjectValue $payload -Name "steps" -DefaultValue @()) |
            Where-Object { $null -ne $_ }
    )
    $stepPass = Test-StepResultsPass -Steps $steps
    $batchCovered = $true
    if (-not [string]::IsNullOrWhiteSpace($BatchDateValue)) {
        $payloadBatchDate = [string](Get-PropValue -ObjectValue $payload -Name "batch_date" -DefaultValue "")
        if (-not [string]::IsNullOrWhiteSpace($payloadBatchDate)) {
            $batchCovered = ($payloadBatchDate.Trim() -eq $BatchDateValue.Trim())
        } else {
        $validateDates = @(
            @(Get-PropValue -ObjectValue $payload -Name "validate_dates" -DefaultValue @()) |
                ForEach-Object { [string]$_ } |
                Where-Object { -not [string]::IsNullOrWhiteSpace($_) }
        )
        if ($validateDates.Count -gt 0) {
            $batchCovered = @($validateDates) -contains $BatchDateValue
        } else {
            $expectedStepName = "validate_raw_ticks_" + $BatchDateValue
            $batchCovered = @($steps | Where-Object { [string](Get-PropValue -ObjectValue $_ -Name "step" -DefaultValue "") -eq $expectedStepName }).Count -gt 0
        }
        }
    }
    $pass = $exists -and ($null -ne $generatedAt) -and ($ageMinutes -le [double]$MaxAgeMinutes) -and $stepPass -and $batchCovered
    if ($DryRun) {
        $pass = $true
    }
    return [ordered]@{
        name = $Name
        summary_path = $SummaryFile
        exists = $exists
        generated_at_utc = $generatedAtUtc
        age_minutes = $ageMinutes
        max_age_minutes = [int]$MaxAgeMinutes
        step_pass = $stepPass
        batch_date = $BatchDateValue
        batch_covered = $batchCovered
        pass = $pass
    }
}

$resolvedProjectRoot = if ([string]::IsNullOrWhiteSpace($ProjectRoot)) { Resolve-DefaultProjectRoot } else { $ProjectRoot }
$resolvedProjectRoot = [System.IO.Path]::GetFullPath($resolvedProjectRoot)
$resolvedPythonExe = if ([string]::IsNullOrWhiteSpace($PythonExe)) { Resolve-DefaultPythonExe -Root $resolvedProjectRoot } else { $PythonExe }
$resolvedSummaryPath = Resolve-ProjectPath -Root $resolvedProjectRoot -PathValue $SummaryPath
$resolvedRefreshSummaryPath = Resolve-ProjectPath -Root $resolvedProjectRoot -PathValue $RefreshSummaryPath
$resolvedFeatureRefreshSummaryPath = Resolve-ProjectPath -Root $resolvedProjectRoot -PathValue $FeatureRefreshSummaryPath
$resolvedDataPlatformRefreshScript = if ([string]::IsNullOrWhiteSpace($DataPlatformRefreshScript)) {
    Join-Path $resolvedProjectRoot "scripts/refresh_data_platform_layers.ps1"
} else {
    Resolve-ProjectPath -Root $resolvedProjectRoot -PathValue $DataPlatformRefreshScript
}
$resolvedFeatureContractRefreshScript = if ([string]::IsNullOrWhiteSpace($FeatureContractRefreshScript)) {
    Join-Path $resolvedProjectRoot "scripts/refresh_current_features_v4_contract_artifacts.ps1"
} else {
    Resolve-ProjectPath -Root $resolvedProjectRoot -PathValue $FeatureContractRefreshScript
}
$resolvedCandlesSummaryPath = Resolve-ProjectPath -Root $resolvedProjectRoot -PathValue $CandlesSummaryPath
$resolvedTicksSummaryPath = Resolve-ProjectPath -Root $resolvedProjectRoot -PathValue $TicksSummaryPath
$resolvedMicroRoot = Resolve-ProjectPath -Root $resolvedProjectRoot -PathValue $MicroRoot
$batchDateValue = Resolve-BatchDateValue -DateText $BatchDate
$pwshExe = Resolve-PwshExe

Set-Location $resolvedProjectRoot

$candlesFreshness = Get-SourceFreshnessResult `
    -Name "candles_api_refresh" `
    -SummaryFile $resolvedCandlesSummaryPath `
    -MaxAgeMinutes $MaxCandlesSummaryAgeMinutes
$ticksFreshness = Get-SourceFreshnessResult `
    -Name "raw_ticks_daily" `
    -SummaryFile $resolvedTicksSummaryPath `
    -MaxAgeMinutes $MaxTicksSummaryAgeMinutes `
    -BatchDateValue $batchDateValue

$failureReasons = @()
if (-not (To-Bool (Get-PropValue -ObjectValue $candlesFreshness -Name "pass" -DefaultValue $false) $false)) {
    $failureReasons += "CANDLES_API_REFRESH_STALE_OR_MISSING"
}
if (-not (To-Bool (Get-PropValue -ObjectValue $ticksFreshness -Name "pass" -DefaultValue $false) $false)) {
    $failureReasons += "RAW_TICKS_DAILY_STALE_OR_MISSING"
}

$refreshExec = $null
$refreshSummary = @{}
if (@($failureReasons).Count -eq 0) {
    $refreshArgs = @(
        "-NoProfile",
        "-ExecutionPolicy", "Bypass",
        "-File", $resolvedDataPlatformRefreshScript,
        "-ProjectRoot", $resolvedProjectRoot,
        "-PythonExe", $resolvedPythonExe,
        "-Mode", "training_critical",
        "-SummaryPath", $resolvedRefreshSummaryPath,
        "-SkipPublishReadySnapshot"
    )
    if ($DryRun) {
        $refreshArgs += "-DryRun"
    }
    $refreshExec = Invoke-CommandCapture -Exe $pwshExe -ArgList $refreshArgs -AllowFailure
    $refreshSummary = Load-JsonOrEmpty -PathValue $resolvedRefreshSummaryPath
    if ([int]$refreshExec.ExitCode -ne 0) {
        $failureReasons += "TRAINING_CRITICAL_REFRESH_FAILED"
    }
}

$featureRefreshExec = $null
$featureRefreshSummary = @{}
if (@($failureReasons).Count -eq 0) {
    $featureRefreshArgs = @(
        "-NoProfile",
        "-ExecutionPolicy", "Bypass",
        "-File", $resolvedFeatureContractRefreshScript,
        "-ProjectRoot", $resolvedProjectRoot,
        "-PythonExe", $resolvedPythonExe,
        "-SummaryPath", $resolvedFeatureRefreshSummaryPath,
        "-SkipMicroRefresh",
        "-SkipMicroValidate",
        "-SkipParity",
        "-SkipRegistryRefresh"
    )
    if ($DryRun) {
        $featureRefreshArgs += "-DryRun"
    }
    $featureRefreshExec = Invoke-CommandCapture -Exe $pwshExe -ArgList $featureRefreshArgs -AllowFailure
    $featureRefreshSummary = Load-JsonOrEmpty -PathValue $resolvedFeatureRefreshSummaryPath
    if ([int]$featureRefreshExec.ExitCode -ne 0) {
        $failureReasons += "FEATURE_CONTRACT_REFRESH_FAILED"
    }
}

$snapshotId = ""
$snapshotRoot = ""
$snapshotSummaryPath = ""
$pointerPath = Join-Path $resolvedProjectRoot "data/_meta/data_platform_ready_snapshot.json"
$snapshotPublishExec = $null
if (@($failureReasons).Count -eq 0) {
    $publishArgs = @(
        "-m", "autobot.ops.data_platform_snapshot",
        "publish",
        "--project-root", $resolvedProjectRoot
    )
    $snapshotPublishExec = Invoke-CommandCapture -Exe $resolvedPythonExe -ArgList $publishArgs -AllowFailure
    if ([int]$snapshotPublishExec.ExitCode -ne 0) {
        $failureReasons += "TRAIN_READY_SNAPSHOT_PUBLISH_FAILED"
    } elseif (-not [string]::IsNullOrWhiteSpace($snapshotPublishExec.Output)) {
        try {
            $publishPayload = $snapshotPublishExec.Output | ConvertFrom-Json
            $snapshotId = [string](Get-PropValue -ObjectValue $publishPayload -Name "snapshot_id" -DefaultValue "")
            $snapshotRoot = [string](Get-PropValue -ObjectValue $publishPayload -Name "snapshot_root" -DefaultValue "")
            $snapshotSummaryPath = [string](Get-PropValue -ObjectValue $publishPayload -Name "summary_path" -DefaultValue "")
        } catch {
            $failureReasons += "TRAIN_READY_SNAPSHOT_PUBLISH_OUTPUT_INVALID"
        }
    }
}

$publishedAtUtc = (Get-Date).ToUniversalTime().ToString("o")
$deadlineDate = [DateTime]::ParseExact($batchDateValue, "yyyy-MM-dd", [System.Globalization.CultureInfo]::InvariantCulture).AddDays(1).AddMinutes(20)
$deadlineMet = $true
if ((-not $DryRun) -and (-not $SkipDeadline)) {
    $deadlineMet = ((Get-Date).ToUniversalTime() -le $deadlineDate.ToUniversalTime())
}
if ((@($failureReasons).Count -eq 0) -and (-not $SkipDeadline) -and (-not $deadlineMet)) {
    $failureReasons += "TRAIN_SNAPSHOT_CLOSE_DEADLINE_MISSED"
}

$microCoverageCounts = Get-MicroDateCoverageCounts -MicroRoot $resolvedMicroRoot -Tf $Tf
$summary = [ordered]@{
    policy = "v5_train_snapshot_close_v1"
    generated_at_utc = (Get-Date).ToUniversalTime().ToString("o")
    batch_date = $batchDateValue
    project_root = $resolvedProjectRoot
    python_exe = $resolvedPythonExe
    source_freshness = [ordered]@{
        candles_api_refresh = $candlesFreshness
        raw_ticks_daily = $ticksFreshness
    }
    training_critical_steps = @(
        @(Get-PropValue -ObjectValue $refreshSummary -Name "steps" -DefaultValue @()) |
            Where-Object { $null -ne $_ }
    )
    training_critical_refresh = [ordered]@{
        attempted = ($null -ne $refreshExec)
        exit_code = if ($null -ne $refreshExec) { [int]$refreshExec.ExitCode } else { 0 }
        command = if ($null -ne $refreshExec) { [string]$refreshExec.Command } else { "" }
        summary_path = $resolvedRefreshSummaryPath
        mode = "training_critical"
    }
    feature_contract_refresh = [ordered]@{
        attempted = ($null -ne $featureRefreshExec)
        exit_code = if ($null -ne $featureRefreshExec) { [int]$featureRefreshExec.ExitCode } else { 0 }
        command = if ($null -ne $featureRefreshExec) { [string]$featureRefreshExec.Command } else { "" }
        summary_path = $resolvedFeatureRefreshSummaryPath
    }
    micro_root = $resolvedMicroRoot
    micro_date_coverage_counts = $microCoverageCounts
    snapshot_id = $snapshotId
    snapshot_root = $snapshotRoot
    snapshot_path = $snapshotRoot
    snapshot_summary_path = $snapshotSummaryPath
    pointer_path = $pointerPath
    published_at_utc = $publishedAtUtc
    deadline_enforced = (-not [bool]$SkipDeadline)
    deadline_met = $deadlineMet
    overall_pass = (@($failureReasons).Count -eq 0)
    failure_reasons = @($failureReasons)
}

$summaryDir = Split-Path -Parent $resolvedSummaryPath
if (-not [string]::IsNullOrWhiteSpace($summaryDir)) {
    New-Item -ItemType Directory -Force -Path $summaryDir | Out-Null
}
($summary | ConvertTo-Json -Depth 10) | Set-Content -Path $resolvedSummaryPath -Encoding UTF8
Write-Host $resolvedSummaryPath
if (@($failureReasons).Count -gt 0) {
    exit 2
}
exit 0
