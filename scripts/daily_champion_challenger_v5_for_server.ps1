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
    [string]$PairedPaperTf = "5m",
    [string]$PairedPaperPreset = "live_v5",
    [string]$PairedPaperModelFamily = "",
    [string]$PairedPaperFeatureSet = "v4",
    [string]$PairedPaperFeatureProvider = "live_v5",
    [string]$PairedPaperMicroProvider = "live_ws",
    [int]$PairedPaperWarmupSec = 60,
    [int]$PairedPaperWarmupMinTradeEventsPerMarket = 1,
    [int]$ExecutionContractMinRows = 20,
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

if ($Mode -ne "promote_only") {
    if (Test-Path $resolvedCandlesRefreshScript) {
        $candlesArgs = @(
            "-ProjectRoot", $resolvedProjectRoot,
            "-PythonExe", $resolvedPythonExe
        )
        if ($DryRun) {
            $candlesArgs += "-DryRun"
        }
        Invoke-CheckedScript -PwshExe $resolvedPwshExe -ScriptPath $resolvedCandlesRefreshScript -ArgsList $candlesArgs -StepName "candles_api_refresh"
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
        Invoke-CheckedScript -PwshExe $resolvedPwshExe -ScriptPath $resolvedRawTicksDailyScript -ArgsList $ticksArgs -StepName "raw_ticks_daily"
    }
    if (Test-Path $resolvedTrainSnapshotCloseScript) {
        $closeArgs = @(
            "-ProjectRoot", $resolvedProjectRoot,
            "-PythonExe", $resolvedPythonExe,
            "-BatchDate", $resolvedBatchDate,
            "-SkipDeadline"
        )
        if ($DryRun) {
            $closeArgs += "-DryRun"
        }
        Invoke-CheckedScript -PwshExe $resolvedPwshExe -ScriptPath $resolvedTrainSnapshotCloseScript -ArgsList $closeArgs -StepName "train_snapshot_close"
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
    -BlockOnActiveUnits $BlockOnActiveUnits `
    -AcceptanceArgs $AcceptanceArgs `
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
    -Mode $Mode `
    -SkipDailyPipeline:$true `
    -SkipFeatureContractRefresh:$true `
    -SkipReportRefresh:$SkipReportRefresh `
    -DryRun:$DryRun `
    @args
exit $LASTEXITCODE
