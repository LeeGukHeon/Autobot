param(
    [string]$ProjectRoot = "",
    [string]$PythonExe = "",
    [string]$CandlesRefreshScript = "",
    [string]$RawTicksDailyScript = "",
    [string]$TrainSnapshotCloseScript = "",
    [string]$AcceptanceScript = "",
    [string]$PairedPaperScript = "",
    [string]$RuntimeInstallScript = "",
    [string]$CandidateAdoptionScript = "",
    [string]$ExecutionPolicyRefreshScript = "",
    [string]$FeatureContractRefreshScript = "",
    [string]$BatchDate = "",
    [string]$ModelFamily = "train_v5_fusion",
    [string]$ChampionCompareModelFamily = "",
    [string]$ChampionUnitName = "autobot-paper-v5.service",
    [string]$ChallengerUnitName = "",
    [string]$PairedPaperUnitName = "autobot-paper-v5-paired.service",
    [string[]]$PromotionTargetUnits = @("autobot-live-alpha.service"),
    [string[]]$CandidateTargetUnits = @("autobot-live-alpha-canary.service"),
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
    [string]$PairedPaperTf = "1m",
    [string]$Tf = "1m",
    [int]$HoldBars = 30,
    [string]$PairedPaperPreset = "live_v5",
    [string]$PairedPaperModelFamily = "",
    [string]$PairedPaperFeatureSet = "v4",
    [string]$PairedPaperFeatureProvider = "live_v5",
    [string]$PairedPaperMicroProvider = "live_ws",
    [int]$PairedPaperWarmupSec = 60,
    [int]$PairedPaperWarmupMinTradeEventsPerMarket = 1,
    [int]$ExecutionContractMinRows = 20,
    [int[]]$ExecutionContractLookbackDays = @(14, 30),
    [ValidateSet("combined", "promote_only", "spawn_only")]
    [string]$Mode = "combined",
    [switch]$SkipDailyPipeline,
    [switch]$SkipFeatureContractRefresh,
    [switch]$SkipReportRefresh,
    [switch]$DryRun
)

$scriptPath = Join-Path $PSScriptRoot "daily_champion_challenger_v4_for_server.ps1"

function Resolve-DefaultCandlesRefreshScript {
    param([string]$Root)
    return (Join-Path $Root "scripts/run_candles_api_refresh.ps1")
}

function Resolve-DefaultRawTicksDailyScript {
    param([string]$Root)
    return (Join-Path $Root "scripts/run_raw_ticks_daily.ps1")
}

function Resolve-DefaultTrainSnapshotCloseScript {
    param([string]$Root)
    return (Join-Path $Root "scripts/close_v5_train_ready_snapshot.ps1")
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

function Invoke-CheckedScript {
    param(
        [string]$PwshExe,
        [string]$ScriptPath,
        [string[]]$ArgsList,
        [string]$StepName
    )
    $argList = @(
        "-NoProfile",
        "-ExecutionPolicy", "Bypass",
        "-File", $ScriptPath
    ) + @($ArgsList)
    Write-Host ("[daily-cc-v5] step={0}" -f $StepName)
    & $PwshExe @argList
    if ($LASTEXITCODE -ne 0) {
        throw ("step failed: " + $StepName + " exit_code=" + $LASTEXITCODE)
    }
}

function Write-V5WrapperFailureReport {
    param(
        [string]$Root,
        [string]$ModeName,
        [string]$BatchDateValue,
        [string]$ModelFamilyName,
        [string]$FailureStage,
        [string]$FailureCode,
        [string]$FailureReportPath,
        [string]$Message,
        [string]$StepName
    )
    $outDir = Join-Path $Root "logs/model_v5_candidate"
    New-Item -ItemType Directory -Force -Path $outDir | Out-Null
    $stamp = Get-Date -Format "yyyyMMdd-HHmmss"
    $runPath = Join-Path $outDir ("daily_cc_wrapper_" + $stamp + ".json")
    $latestPath = Join-Path $outDir "latest.json"
    $payload = [ordered]@{
        mode = $ModeName
        batch_date = $BatchDateValue
        started_at_utc = (Get-Date).ToUniversalTime().ToString("o")
        completed_at_utc = (Get-Date).ToUniversalTime().ToString("o")
        model_family = $ModelFamilyName
        failure_stage = $FailureStage
        failure_code = $FailureCode
        failure_report_path = $FailureReportPath
        steps = [ordered]@{
            pre_chain = [ordered]@{
                attempted = $true
                step = $StepName
                message = $Message
            }
        }
        exception = [ordered]@{
            message = $Message
        }
    }
    ($payload | ConvertTo-Json -Depth 10) | Set-Content -Path $runPath -Encoding UTF8
    ($payload | ConvertTo-Json -Depth 10) | Set-Content -Path $latestPath -Encoding UTF8
    Write-Host ("[daily-cc-v5] report={0}" -f $runPath)
    Write-Host ("[daily-cc-v5] latest={0}" -f $latestPath)
}

$resolvedAdoptionScript = if ([string]::IsNullOrWhiteSpace($CandidateAdoptionScript)) {
    (Join-Path $PSScriptRoot "adopt_v5_candidate_for_server.ps1")
} else {
    $CandidateAdoptionScript
}
$resolvedProjectRoot = if ([string]::IsNullOrWhiteSpace($ProjectRoot)) {
    Split-Path -Path $PSScriptRoot -Parent
} else {
    [System.IO.Path]::GetFullPath($ProjectRoot)
}
$resolvedPythonExe = if ([string]::IsNullOrWhiteSpace($PythonExe)) {
    if ([System.IO.Path]::DirectorySeparatorChar -eq '\') {
        "C:\Python314\python.exe"
    } else {
        (Join-Path $resolvedProjectRoot ".venv/bin/python")
    }
} else {
    $PythonExe
}
$resolvedCandlesRefreshScript = if ([string]::IsNullOrWhiteSpace($CandlesRefreshScript)) {
    Resolve-DefaultCandlesRefreshScript -Root $resolvedProjectRoot
} else {
    $CandlesRefreshScript
}
$resolvedRawTicksDailyScript = if ([string]::IsNullOrWhiteSpace($RawTicksDailyScript)) {
    Resolve-DefaultRawTicksDailyScript -Root $resolvedProjectRoot
} else {
    $RawTicksDailyScript
}
$resolvedTrainSnapshotCloseScript = if ([string]::IsNullOrWhiteSpace($TrainSnapshotCloseScript)) {
    Resolve-DefaultTrainSnapshotCloseScript -Root $resolvedProjectRoot
} else {
    $TrainSnapshotCloseScript
}
$resolvedPwshExe = if ([System.IO.Path]::DirectorySeparatorChar -eq '\') {
    "powershell.exe"
} else {
    $cmd = Get-Command pwsh -ErrorAction SilentlyContinue
    if ($null -ne $cmd -and -not [string]::IsNullOrWhiteSpace($cmd.Source)) {
        [string]$cmd.Source
    } else {
        "pwsh"
    }
}
$resolvedBatchDate = Resolve-BatchDateValue -DateText $BatchDate
$resolvedAcceptanceArgs = @()
foreach ($item in @($AcceptanceArgs)) {
    $text = [string]$item
    if ([string]::IsNullOrWhiteSpace($text)) {
        continue
    }
    $resolvedAcceptanceArgs += $text
}
$acceptanceHasTfOverride = $false
$acceptanceHasHoldBarsOverride = $false
foreach ($item in @($resolvedAcceptanceArgs)) {
    $normalized = ([string]$item).Trim().ToLowerInvariant()
    if (($normalized -eq "-tf") -or ($normalized -eq "--tf")) {
        $acceptanceHasTfOverride = $true
    }
    if (($normalized -eq "-holdbars") -or ($normalized -eq "--holdbars") -or ($normalized -eq "--hold-bars")) {
        $acceptanceHasHoldBarsOverride = $true
    }
}
if (-not $acceptanceHasTfOverride) {
    $resolvedAcceptanceArgs += @("-Tf", ([string]$Tf).Trim().ToLowerInvariant())
}
if (-not $acceptanceHasHoldBarsOverride) {
    $resolvedAcceptanceArgs += @("-HoldBars", ([string]([Math]::Max([int]$HoldBars, 1))))
}
$resolvedPreflightExpectedEnabledUnits = @(
    [string]$ChampionUnitName,
    [string]$PairedPaperUnitName
) + @($CandidateTargetUnits) + @(
    "autobot-v5-challenger-spawn.timer",
    "autobot-v5-challenger-promote.timer"
)
$resolvedPreflightExpectedDisabledUnits = @(
    "autobot-paper-v4-replay.service",
    "autobot-live-alpha-replay-shadow.service"
)
$serializedPreflightExpectedEnabledUnits = [string]::Join(",", @(
    $resolvedPreflightExpectedEnabledUnits |
        Where-Object { -not [string]::IsNullOrWhiteSpace([string]$_) } |
        ForEach-Object { ([string]$_).Trim() }
))
$serializedPreflightExpectedDisabledUnits = [string]::Join(",", @(
    $resolvedPreflightExpectedDisabledUnits |
        Where-Object { -not [string]::IsNullOrWhiteSpace([string]$_) } |
        ForEach-Object { ([string]$_).Trim() }
))

if ($Mode -ne "promote_only") {
    if (Test-Path $resolvedCandlesRefreshScript) {
        $candlesArgs = @(
            "-ProjectRoot", $resolvedProjectRoot,
            "-PythonExe", $resolvedPythonExe
        )
        if ($DryRun) {
            $candlesArgs += "-DryRun"
        }
        try {
            Invoke-CheckedScript -PwshExe $resolvedPwshExe -ScriptPath $resolvedCandlesRefreshScript -ArgsList $candlesArgs -StepName "candles_api_refresh"
        } catch {
            Write-V5WrapperFailureReport `
                -Root $resolvedProjectRoot `
                -ModeName $Mode `
                -BatchDateValue $resolvedBatchDate `
                -ModelFamilyName $ModelFamily `
                -FailureStage "data_close" `
                -FailureCode "CANDLES_API_REFRESH_FAILED" `
                -FailureReportPath (Join-Path $resolvedProjectRoot "data/collect/_meta/candles_api_refresh_latest.json") `
                -Message $_.Exception.Message `
                -StepName "candles_api_refresh"
            throw
        }
    }
    if (Test-Path $resolvedRawTicksDailyScript) {
        $ticksArgs = @(
            "-ProjectRoot", $resolvedProjectRoot,
            "-PythonExe", $resolvedPythonExe,
            "-BatchDate", $resolvedBatchDate
        )
        if ($DryRun) {
            $ticksArgs += "-DryRun"
        }
        try {
            Invoke-CheckedScript -PwshExe $resolvedPwshExe -ScriptPath $resolvedRawTicksDailyScript -ArgsList $ticksArgs -StepName "raw_ticks_daily"
        } catch {
            Write-V5WrapperFailureReport `
                -Root $resolvedProjectRoot `
                -ModeName $Mode `
                -BatchDateValue $resolvedBatchDate `
                -ModelFamilyName $ModelFamily `
                -FailureStage "data_close" `
                -FailureCode "RAW_TICKS_DAILY_FAILED" `
                -FailureReportPath (Join-Path $resolvedProjectRoot "data/raw_ticks/upbit/_meta/ticks_daily_latest.json") `
                -Message $_.Exception.Message `
                -StepName "raw_ticks_daily"
            throw
        }
    }
    if (Test-Path $resolvedTrainSnapshotCloseScript) {
        $closeArgs = @(
            "-ProjectRoot", $resolvedProjectRoot,
            "-PythonExe", $resolvedPythonExe,
            "-BatchDate", $resolvedBatchDate,
            "-Tf", ([string]$Tf).Trim().ToLowerInvariant(),
            "-SkipDeadline"
        )
        if ($DryRun) {
            $closeArgs += "-DryRun"
        }
        try {
            Invoke-CheckedScript -PwshExe $resolvedPwshExe -ScriptPath $resolvedTrainSnapshotCloseScript -ArgsList $closeArgs -StepName "train_snapshot_close"
        } catch {
            Write-V5WrapperFailureReport `
                -Root $resolvedProjectRoot `
                -ModeName $Mode `
                -BatchDateValue $resolvedBatchDate `
                -ModelFamilyName $ModelFamily `
                -FailureStage "data_close" `
                -FailureCode "TRAIN_SNAPSHOT_CLOSE_FAILED" `
                -FailureReportPath (Join-Path $resolvedProjectRoot "data/collect/_meta/train_snapshot_close_latest.json") `
                -Message $_.Exception.Message `
                -StepName "train_snapshot_close"
            throw
        }
    }
}

& $scriptPath `
    -ProjectRoot $resolvedProjectRoot `
    -PythonExe $resolvedPythonExe `
    -AcceptanceScript $AcceptanceScript `
    -PairedPaperScript $PairedPaperScript `
    -RuntimeInstallScript $RuntimeInstallScript `
    -CandidateAdoptionScript $resolvedAdoptionScript `
    -ExecutionPolicyRefreshScript $ExecutionPolicyRefreshScript `
    -FeatureContractRefreshScript $FeatureContractRefreshScript `
    -BatchDate $resolvedBatchDate `
    -ModelFamily $ModelFamily `
    -ChampionCompareModelFamily $ChampionCompareModelFamily `
    -ChampionUnitName $ChampionUnitName `
    -ChallengerUnitName "" `
    -PairedPaperUnitName $PairedPaperUnitName `
    -CanaryUnitName "autobot-live-alpha-canary.service" `
    -StateRootRelPath "logs/model_v5_candidate" `
    -CandidateStateDbPath "data/state/live_canary/live_state.db" `
    -SpawnServiceUnitName "autobot-v5-challenger-spawn.service" `
    -PromoteServiceUnitName "autobot-v5-challenger-promote.service" `
    -SpawnTimerUnitName "autobot-v5-challenger-spawn.timer" `
    -PromoteTimerUnitName "autobot-v5-challenger-promote.timer" `
    -PromotionTargetUnits $PromotionTargetUnits `
    -CandidateTargetUnits $CandidateTargetUnits `
    -PreflightExpectedEnabledUnits $serializedPreflightExpectedEnabledUnits `
    -PreflightExpectedDisabledUnits $serializedPreflightExpectedDisabledUnits `
    -BlockOnActiveUnits $BlockOnActiveUnits `
    -AcceptanceArgs $resolvedAcceptanceArgs `
    -ChallengerMinHours $ChallengerMinHours `
    -ChallengerMinOrdersFilled $ChallengerMinOrdersFilled `
    -ChallengerMinRealizedPnlQuote $ChallengerMinRealizedPnlQuote `
    -ChallengerMinMicroQualityScore $ChallengerMinMicroQualityScore `
    -ChallengerMinNonnegativeRatio $ChallengerMinNonnegativeRatio `
    -ChallengerMaxDrawdownDeteriorationFactor $ChallengerMaxDrawdownDeteriorationFactor `
    -ChallengerMicroQualityTolerance $ChallengerMicroQualityTolerance `
    -ChallengerNonnegativeRatioTolerance $ChallengerNonnegativeRatioTolerance `
    -PairedPaperDurationSec $PairedPaperDurationSec `
    -PairedPaperMinMatchedOpportunities $PairedPaperMinMatchedOpportunities `
    -PairedPaperQuote $PairedPaperQuote `
    -PairedPaperTopN $PairedPaperTopN `
    -PairedPaperTf $PairedPaperTf `
    -PairedPaperPreset $PairedPaperPreset `
    -PairedPaperModelFamily $PairedPaperModelFamily `
    -PairedPaperFeatureSet $PairedPaperFeatureSet `
    -PairedPaperFeatureProvider $PairedPaperFeatureProvider `
    -PairedPaperMicroProvider $PairedPaperMicroProvider `
    -PairedPaperWarmupSec $PairedPaperWarmupSec `
    -PairedPaperWarmupMinTradeEventsPerMarket $PairedPaperWarmupMinTradeEventsPerMarket `
    -ExecutionContractMinRows $ExecutionContractMinRows `
    -ExecutionContractLookbackDays $ExecutionContractLookbackDays `
    -Mode $Mode `
    -SkipDailyPipeline:$true `
    -SkipFeatureContractRefresh:$true `
    -SkipReportRefresh:$SkipReportRefresh `
    -DryRun:$DryRun `
    @args
exit $LASTEXITCODE
