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
    [string]$Tf = "1m",
    [int]$FeatureTopN = 50,
    [int]$TrainLookbackDays = 30,
    [int]$BacktestLookbackDays = 8,
    [string]$TrainingCriticalStartDate = "",
    [string]$TrainingCriticalEndDate = "",
    [int]$MaxCandlesSummaryAgeMinutes = 120,
    [int]$MaxTicksSummaryAgeMinutes = 120,
    [switch]$ForceRebuild,
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

function Get-StringArray {
    param([Parameter(Mandatory = $false)]$Value)
    if ($null -eq $Value) {
        return @()
    }
    if (($Value -is [System.Collections.IEnumerable]) -and (-not ($Value -is [string]))) {
        return @(
            @($Value) |
                ForEach-Object { [string]$_ } |
                Where-Object { -not [string]::IsNullOrWhiteSpace($_) }
        )
    }
    $text = [string]$Value
    if ([string]::IsNullOrWhiteSpace($text)) {
        return @()
    }
    return @($text)
}

function Test-WindowContractMatch {
    param(
        [Parameter(Mandatory = $false)]$ExistingWindow,
        [Parameter(Mandatory = $false)]$ExpectedWindow
    )
    $existingStart = [string](Get-PropValue -ObjectValue $ExistingWindow -Name "start" -DefaultValue "")
    $existingEnd = [string](Get-PropValue -ObjectValue $ExistingWindow -Name "end" -DefaultValue "")
    $expectedStart = [string](Get-PropValue -ObjectValue $ExpectedWindow -Name "start" -DefaultValue "")
    $expectedEnd = [string](Get-PropValue -ObjectValue $ExpectedWindow -Name "end" -DefaultValue "")
    return (
        [string]::Equals($existingStart.Trim(), $expectedStart.Trim(), [System.StringComparison]::OrdinalIgnoreCase) `
        -and [string]::Equals($existingEnd.Trim(), $expectedEnd.Trim(), [System.StringComparison]::OrdinalIgnoreCase)
    )
}

function Get-ReusableTrainSnapshotCloseSummary {
    param(
        [string]$SummaryFile,
        [string]$PointerFile,
        [string]$ProjectRootValue,
        [string]$BatchDateValue,
        [string]$TrainingCriticalStartDateValue,
        [string]$TrainingCriticalEndDateValue,
        [Parameter(Mandatory = $false)]$ExpectedTrainWindow,
        [Parameter(Mandatory = $false)]$ExpectedCertificationWindow,
        [Parameter(Mandatory = $false)]$ExpectedCoverageWindow,
        [int]$ExpectedFeatureTopN
    )
    if ($ForceRebuild -or $DryRun) {
        return $null
    }
    $existingSummary = Load-JsonOrEmpty -PathValue $SummaryFile
    if ($null -eq $existingSummary) {
        return $null
    }
    $existingPolicy = [string](Get-PropValue -ObjectValue $existingSummary -Name "policy" -DefaultValue "")
    if (-not [string]::Equals($existingPolicy.Trim(), "v5_train_snapshot_close_v1", [System.StringComparison]::OrdinalIgnoreCase)) {
        return $null
    }
    $existingBatchDate = [string](Get-PropValue -ObjectValue $existingSummary -Name "batch_date" -DefaultValue "")
    if (-not [string]::Equals($existingBatchDate.Trim(), $BatchDateValue.Trim(), [System.StringComparison]::OrdinalIgnoreCase)) {
        return $null
    }
    if (-not (To-Bool (Get-PropValue -ObjectValue $existingSummary -Name "overall_pass" -DefaultValue $false) $false)) {
        return $null
    }
    if (@(Get-StringArray -Value (Get-PropValue -ObjectValue $existingSummary -Name "failure_reasons" -DefaultValue @())).Count -gt 0) {
        return $null
    }
    $existingStartDate = [string](Get-PropValue -ObjectValue $existingSummary -Name "training_critical_start_date" -DefaultValue "")
    $existingEndDate = [string](Get-PropValue -ObjectValue $existingSummary -Name "training_critical_end_date" -DefaultValue "")
    if (
        (-not [string]::Equals($existingStartDate.Trim(), $TrainingCriticalStartDateValue.Trim(), [System.StringComparison]::OrdinalIgnoreCase)) `
        -or (-not [string]::Equals($existingEndDate.Trim(), $TrainingCriticalEndDateValue.Trim(), [System.StringComparison]::OrdinalIgnoreCase))
    ) {
        return $null
    }
    if (-not (Test-WindowContractMatch -ExistingWindow (Get-PropValue -ObjectValue $existingSummary -Name "train_window" -DefaultValue @{}) -ExpectedWindow $ExpectedTrainWindow)) {
        return $null
    }
    if (-not (Test-WindowContractMatch -ExistingWindow (Get-PropValue -ObjectValue $existingSummary -Name "certification_window" -DefaultValue @{}) -ExpectedWindow $ExpectedCertificationWindow)) {
        return $null
    }
    if (-not (Test-WindowContractMatch -ExistingWindow (Get-PropValue -ObjectValue $existingSummary -Name "coverage_window" -DefaultValue @{}) -ExpectedWindow $ExpectedCoverageWindow)) {
        return $null
    }
    $existingFeatureTopN = [int](Get-PropValue -ObjectValue (Get-PropValue -ObjectValue $existingSummary -Name "feature_contract_refresh" -DefaultValue @{}) -Name "top_n" -DefaultValue 0)
    if (($ExpectedFeatureTopN -gt 0) -and ($existingFeatureTopN -ne [int]$ExpectedFeatureTopN)) {
        return $null
    }
    $snapshotId = [string](Get-PropValue -ObjectValue $existingSummary -Name "snapshot_id" -DefaultValue "")
    if ([string]::IsNullOrWhiteSpace($snapshotId)) {
        return $null
    }
    $snapshotRoot = [string](Get-PropValue -ObjectValue $existingSummary -Name "snapshot_root" -DefaultValue "")
    if ([string]::IsNullOrWhiteSpace($snapshotRoot) -or (-not (Test-Path $snapshotRoot))) {
        return $null
    }
    $snapshotSummaryPath = [string](Get-PropValue -ObjectValue $existingSummary -Name "snapshot_summary_path" -DefaultValue "")
    if ([string]::IsNullOrWhiteSpace($snapshotSummaryPath)) {
        $snapshotSummaryPath = Join-Path $ProjectRootValue ("data/snapshots/data_platform/" + $snapshotId + "/_meta/summary.json")
    }
    if (-not (Test-Path $snapshotSummaryPath)) {
        return $null
    }
    $pointerPayload = Load-JsonOrEmpty -PathValue $PointerFile
    $pointerSnapshotId = [string](Get-PropValue -ObjectValue $pointerPayload -Name "snapshot_id" -DefaultValue "")
    if (-not [string]::Equals($pointerSnapshotId.Trim(), $snapshotId.Trim(), [System.StringComparison]::OrdinalIgnoreCase)) {
        return $null
    }
    return [PSCustomObject]@{
        Summary = $existingSummary
        SnapshotId = $snapshotId
        SnapshotRoot = $snapshotRoot
        SnapshotSummaryPath = $snapshotSummaryPath
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

function Resolve-DateWindowForTrainingCriticalRefresh {
    param(
        [string]$BatchDateValue,
        [int]$RequestedTrainLookbackDays,
        [int]$RequestedBacktestLookbackDays,
        [string]$ExplicitStartDate,
        [string]$ExplicitEndDate
    )
    if ((-not [string]::IsNullOrWhiteSpace($ExplicitStartDate)) -and (-not [string]::IsNullOrWhiteSpace($ExplicitEndDate))) {
        return [ordered]@{
            start_date = Resolve-BatchDateValue -DateText $ExplicitStartDate
            end_date = Resolve-BatchDateValue -DateText $ExplicitEndDate
            source = "explicit_window"
        }
    }
    $resolvedBatchDate = Resolve-BatchDateValue -DateText $BatchDateValue
    $batchDateObj = [DateTime]::ParseExact($resolvedBatchDate, "yyyy-MM-dd", [System.Globalization.CultureInfo]::InvariantCulture)
    $trainDays = [Math]::Max([int]$RequestedTrainLookbackDays, 1)
    $backtestDays = [Math]::Max([int]$RequestedBacktestLookbackDays, 1)
    $totalDays = [Math]::Max(($trainDays + $backtestDays), 1)
    return [ordered]@{
        start_date = $batchDateObj.AddDays(-1 * ($totalDays - 1)).ToString("yyyy-MM-dd")
        end_date = $resolvedBatchDate
        source = "batch_date_plus_train_and_backtest_lookback"
    }
}

function Resolve-TrainSnapshotWindowContract {
    param(
        [string]$BatchDateValue,
        [int]$RequestedTrainLookbackDays,
        [int]$RequestedBacktestLookbackDays,
        [string]$CoverageStartDate,
        [string]$CoverageEndDate,
        [string]$CoverageSource
    )
    $resolvedBatchDate = Resolve-BatchDateValue -DateText $BatchDateValue
    $batchDateObj = [DateTime]::ParseExact($resolvedBatchDate, "yyyy-MM-dd", [System.Globalization.CultureInfo]::InvariantCulture)
    $trainDays = [Math]::Max([int]$RequestedTrainLookbackDays, 1)
    $backtestDays = [Math]::Max([int]$RequestedBacktestLookbackDays, 1)
    $certificationStartDate = $batchDateObj.AddDays(-1 * ($backtestDays - 1)).ToString("yyyy-MM-dd")
    $trainEndDate = $batchDateObj.AddDays(-1 * $backtestDays).ToString("yyyy-MM-dd")
    $trainEndObj = [DateTime]::ParseExact($trainEndDate, "yyyy-MM-dd", [System.Globalization.CultureInfo]::InvariantCulture)
    $trainStartDate = $trainEndObj.AddDays(-1 * ($trainDays - 1)).ToString("yyyy-MM-dd")
    if (-not [string]::IsNullOrWhiteSpace($CoverageStartDate)) {
        $coverageStartObj = [DateTime]::ParseExact($CoverageStartDate, "yyyy-MM-dd", [System.Globalization.CultureInfo]::InvariantCulture)
        if ($coverageStartObj -gt $trainEndObj) {
            $trainStartDate = ""
        } elseif ($coverageStartObj -gt [DateTime]::ParseExact($trainStartDate, "yyyy-MM-dd", [System.Globalization.CultureInfo]::InvariantCulture)) {
            $trainStartDate = $CoverageStartDate
        }
    }
    return [ordered]@{
        batch_date = $resolvedBatchDate
        train_lookback_days = [int]$trainDays
        certification_lookback_days = [int]$backtestDays
        train_window = [ordered]@{
            start = $trainStartDate
            end = $trainEndDate
        }
        certification_window = [ordered]@{
            start = $certificationStartDate
            end = $resolvedBatchDate
        }
        coverage_window = [ordered]@{
            start = $CoverageStartDate
            end = $CoverageEndDate
        }
        coverage_window_source = [string]$CoverageSource
    }
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
$pointerPath = Join-Path $resolvedProjectRoot "data/_meta/data_platform_ready_snapshot.json"
$trainingCriticalWindow = Resolve-DateWindowForTrainingCriticalRefresh `
    -BatchDateValue $batchDateValue `
    -RequestedTrainLookbackDays $TrainLookbackDays `
    -RequestedBacktestLookbackDays $BacktestLookbackDays `
    -ExplicitStartDate $TrainingCriticalStartDate `
    -ExplicitEndDate $TrainingCriticalEndDate
$trainingCriticalStartDate = [string](Get-PropValue -ObjectValue $trainingCriticalWindow -Name "start_date" -DefaultValue "")
$trainingCriticalEndDate = [string](Get-PropValue -ObjectValue $trainingCriticalWindow -Name "end_date" -DefaultValue "")
$windowContract = Resolve-TrainSnapshotWindowContract `
    -BatchDateValue $batchDateValue `
    -RequestedTrainLookbackDays $TrainLookbackDays `
    -RequestedBacktestLookbackDays $BacktestLookbackDays `
    -CoverageStartDate $trainingCriticalStartDate `
    -CoverageEndDate $trainingCriticalEndDate `
    -CoverageSource ([string](Get-PropValue -ObjectValue $trainingCriticalWindow -Name "source" -DefaultValue ""))
$pwshExe = Resolve-PwshExe

$reusableSummary = Get-ReusableTrainSnapshotCloseSummary `
    -SummaryFile $resolvedSummaryPath `
    -PointerFile $pointerPath `
    -ProjectRootValue $resolvedProjectRoot `
    -BatchDateValue $batchDateValue `
    -TrainingCriticalStartDateValue $trainingCriticalStartDate `
    -TrainingCriticalEndDateValue $trainingCriticalEndDate `
    -ExpectedTrainWindow (Get-PropValue -ObjectValue $windowContract -Name "train_window" -DefaultValue @{}) `
    -ExpectedCertificationWindow (Get-PropValue -ObjectValue $windowContract -Name "certification_window" -DefaultValue @{}) `
    -ExpectedCoverageWindow (Get-PropValue -ObjectValue $windowContract -Name "coverage_window" -DefaultValue @{}) `
    -ExpectedFeatureTopN ([Math]::Max([int]$FeatureTopN, 1))
if ($null -ne $reusableSummary) {
    $reuseSourceGeneratedAt = [string](Get-PropValue -ObjectValue $reusableSummary.Summary -Name "generated_at_utc" -DefaultValue "")
    $reusedSummary = [ordered]@{}
    foreach ($property in @($reusableSummary.Summary.PSObject.Properties)) {
        if ($null -eq $property -or [string]::IsNullOrWhiteSpace([string]$property.Name)) {
            continue
        }
        $reusedSummary[[string]$property.Name] = $property.Value
    }
    $reusedSummary.generated_at_utc = (Get-Date).ToUniversalTime().ToString("o")
    $reusedSummary.reused_existing_summary = $true
    $reusedSummary.reused_source_generated_at_utc = $reuseSourceGeneratedAt
    $reusedSummary.reuse_reason = "MATCHING_BATCH_DATE_SNAPSHOT_CLOSE"
    $reusedSummary.reuse_snapshot_id = [string]$reusableSummary.SnapshotId
    $reusedSummary.reuse_snapshot_summary_path = [string]$reusableSummary.SnapshotSummaryPath
    ($reusedSummary | ConvertTo-Json -Depth 10) | Set-Content -Path $resolvedSummaryPath -Encoding UTF8
    Write-Host ("[train-snapshot-close] reuse_snapshot_id={0}" -f [string]$reusableSummary.SnapshotId)
    Write-Host ("[train-snapshot-close] reuse_snapshot_summary_path={0}" -f [string]$reusableSummary.SnapshotSummaryPath)
    Write-Host $resolvedSummaryPath
    exit 0
}

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
        "-TensorStartDate", $trainingCriticalStartDate,
        "-TensorEndDate", $trainingCriticalEndDate,
        "-MicroStartDate", $trainingCriticalStartDate,
        "-MicroEndDate", $trainingCriticalEndDate,
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
$featureBuildReportPath = Join-Path $resolvedProjectRoot "data/features/features_v4/_meta/build_report.json"
$featureBuildReport = @{}
if (@($failureReasons).Count -eq 0) {
    $featureRefreshArgs = @(
        "-NoProfile",
        "-ExecutionPolicy", "Bypass",
        "-File", $resolvedFeatureContractRefreshScript,
        "-ProjectRoot", $resolvedProjectRoot,
        "-PythonExe", $resolvedPythonExe,
        "-Tf", ([string]$Tf).Trim().ToLowerInvariant(),
        "-StartDate", $trainingCriticalStartDate,
        "-EndDate", $trainingCriticalEndDate,
        "-TopN", ([string]([Math]::Max([int]$FeatureTopN, 1))),
        "-UseTopNUniverse",
        "-RequireExplicitWindow",
        "-SummaryPath", $resolvedFeatureRefreshSummaryPath,
        "-SkipMicroRefresh",
        "-SkipMicroValidate"
    )
    if ($DryRun) {
        $featureRefreshArgs += "-DryRun"
    }
    $featureRefreshExec = Invoke-CommandCapture -Exe $pwshExe -ArgList $featureRefreshArgs -AllowFailure
    $featureRefreshSummary = Load-JsonOrEmpty -PathValue $resolvedFeatureRefreshSummaryPath
    $featureBuildReport = Load-JsonOrEmpty -PathValue $featureBuildReportPath
    if ([int]$featureRefreshExec.ExitCode -ne 0) {
        $failureReasons += "FEATURE_CONTRACT_REFRESH_FAILED"
    }
}

$snapshotId = ""
$snapshotRoot = ""
$snapshotSummaryPath = ""
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
        start_date = [string](Get-PropValue -ObjectValue $refreshSummary -Name "start_date" -DefaultValue $trainingCriticalStartDate)
        end_date = [string](Get-PropValue -ObjectValue $refreshSummary -Name "end_date" -DefaultValue $trainingCriticalEndDate)
        train_window = (Get-PropValue -ObjectValue $windowContract -Name "train_window" -DefaultValue @{})
        certification_window = (Get-PropValue -ObjectValue $windowContract -Name "certification_window" -DefaultValue @{})
        coverage_window = (Get-PropValue -ObjectValue $windowContract -Name "coverage_window" -DefaultValue @{})
        window_source = [string](Get-PropValue -ObjectValue $refreshSummary -Name "window_source" -DefaultValue (Get-PropValue -ObjectValue $trainingCriticalWindow -Name "source" -DefaultValue ""))
        coverage_window_source = [string](Get-PropValue -ObjectValue $trainingCriticalWindow -Name "source" -DefaultValue "")
        refresh_argument_mode = [string](Get-PropValue -ObjectValue $refreshSummary -Name "refresh_argument_mode" -DefaultValue "")
        tensor_dates = @((Get-PropValue -ObjectValue $refreshSummary -Name "tensor_dates" -DefaultValue @()))
        micro_dates = @((Get-PropValue -ObjectValue $refreshSummary -Name "micro_dates" -DefaultValue @()))
        top_n = [int](Get-PropValue -ObjectValue $refreshSummary -Name "top_n" -DefaultValue 0)
        tensor_max_markets_effective = [int](Get-PropValue -ObjectValue $refreshSummary -Name "tensor_max_markets_effective" -DefaultValue 0)
    }
    feature_contract_refresh = [ordered]@{
        attempted = ($null -ne $featureRefreshExec)
        exit_code = if ($null -ne $featureRefreshExec) { [int]$featureRefreshExec.ExitCode } else { 0 }
        command = if ($null -ne $featureRefreshExec) { [string]$featureRefreshExec.Command } else { "" }
        summary_path = $resolvedFeatureRefreshSummaryPath
        start_date = [string](Get-PropValue -ObjectValue $featureRefreshSummary -Name "start" -DefaultValue $trainingCriticalStartDate)
        end_date = [string](Get-PropValue -ObjectValue $featureRefreshSummary -Name "end" -DefaultValue $trainingCriticalEndDate)
        refresh_argument_mode = [string](Get-PropValue -ObjectValue $featureRefreshSummary -Name "refresh_argument_mode" -DefaultValue "")
        top_n = [int](Get-PropValue -ObjectValue $featureRefreshSummary -Name "top_n" -DefaultValue ([Math]::Max([int]$FeatureTopN, 1)))
        universe_mode = [string](Get-PropValue -ObjectValue $featureRefreshSummary -Name "universe_mode" -DefaultValue "")
        features_v4_effective_start = [string](Get-PropValue -ObjectValue $featureBuildReport -Name "effective_start" -DefaultValue "")
        features_v4_effective_end = [string](Get-PropValue -ObjectValue $featureBuildReport -Name "effective_end" -DefaultValue "")
    }
    micro_root = $resolvedMicroRoot
    micro_date_coverage_counts = $microCoverageCounts
    train_window = (Get-PropValue -ObjectValue $windowContract -Name "train_window" -DefaultValue @{})
    certification_window = (Get-PropValue -ObjectValue $windowContract -Name "certification_window" -DefaultValue @{})
    coverage_window = (Get-PropValue -ObjectValue $windowContract -Name "coverage_window" -DefaultValue @{})
    training_critical_start_date = $trainingCriticalStartDate
    training_critical_end_date = $trainingCriticalEndDate
    coverage_window_source = [string](Get-PropValue -ObjectValue $trainingCriticalWindow -Name "source" -DefaultValue "")
    refresh_argument_mode = [string](Get-PropValue -ObjectValue $refreshSummary -Name "refresh_argument_mode" -DefaultValue "")
    tensor_dates = @((Get-PropValue -ObjectValue $refreshSummary -Name "tensor_dates" -DefaultValue @()))
    micro_dates = @((Get-PropValue -ObjectValue $refreshSummary -Name "micro_dates" -DefaultValue @()))
    top_n = [int](Get-PropValue -ObjectValue $refreshSummary -Name "top_n" -DefaultValue 0)
    tensor_max_markets_effective = [int](Get-PropValue -ObjectValue $refreshSummary -Name "tensor_max_markets_effective" -DefaultValue 0)
    features_v4_effective_start = [string](Get-PropValue -ObjectValue $featureBuildReport -Name "effective_start" -DefaultValue "")
    features_v4_effective_end = [string](Get-PropValue -ObjectValue $featureBuildReport -Name "effective_end" -DefaultValue "")
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
