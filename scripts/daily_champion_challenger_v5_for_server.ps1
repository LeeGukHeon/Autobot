param(
    [string]$ProjectRoot = "",
    [string]$PythonExe = "",
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
$resolvedAdoptionScript = if ([string]::IsNullOrWhiteSpace($CandidateAdoptionScript)) {
    (Join-Path $PSScriptRoot "adopt_v5_candidate_for_server.ps1")
} else {
    $CandidateAdoptionScript
}
& $scriptPath `
    -ProjectRoot $ProjectRoot `
    -PythonExe $PythonExe `
    -AcceptanceScript $AcceptanceScript `
    -PairedPaperScript $PairedPaperScript `
    -RuntimeInstallScript $RuntimeInstallScript `
    -CandidateAdoptionScript $resolvedAdoptionScript `
    -ExecutionPolicyRefreshScript $ExecutionPolicyRefreshScript `
    -FeatureContractRefreshScript $FeatureContractRefreshScript `
    -BatchDate $BatchDate `
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
    -SkipDailyPipeline:$SkipDailyPipeline `
    -SkipFeatureContractRefresh:$true `
    -SkipReportRefresh:$SkipReportRefresh `
    -DryRun:$DryRun `
    @args
exit $LASTEXITCODE
