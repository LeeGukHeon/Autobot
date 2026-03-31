param(
    [string]$ProjectRoot = "",
    [string]$PythonExe = "",
    [string]$RuntimeInstallScript = "",
    [string]$BatchDate = "",
    [string]$CandidateRunId = "",
    [string]$ModelFamily = "train_v5_fusion",
    [string]$ChampionCompareModelFamily = "",
    [string]$ChampionUnitName = "autobot-paper-v5.service",
    [string]$PairedPaperUnitName = "autobot-paper-v5-paired.service",
    [string[]]$PromotionTargetUnits = @("autobot-live-alpha.service"),
    [string[]]$CandidateTargetUnits = @("autobot-live-alpha-canary.service"),
    [string]$LaneMode = "",
    [bool]$PromotionEligible = $true,
    [switch]$BootstrapOnly,
    [string]$SplitPolicyId = "",
    [string]$SplitPolicyArtifactPath = "",
    [switch]$DryRun
)

$scriptPath = Join-Path $PSScriptRoot "adopt_v4_candidate_for_server.ps1"
& $scriptPath `
    -ProjectRoot $ProjectRoot `
    -PythonExe $PythonExe `
    -RuntimeInstallScript $RuntimeInstallScript `
    -BatchDate $BatchDate `
    -CandidateRunId $CandidateRunId `
    -ModelFamily $ModelFamily `
    -StateRootRelPath "logs/model_v5_candidate" `
    -ChampionCompareModelFamily $ChampionCompareModelFamily `
    -ChampionUnitName $ChampionUnitName `
    -PairedPaperUnitName $PairedPaperUnitName `
    -PromotionTargetUnits $PromotionTargetUnits `
    -CandidateTargetUnits $CandidateTargetUnits `
    -LaneMode $LaneMode `
    -PromotionEligible:$PromotionEligible `
    -BootstrapOnly:$BootstrapOnly `
    -SplitPolicyId $SplitPolicyId `
    -SplitPolicyArtifactPath $SplitPolicyArtifactPath `
    -DryRun:$DryRun `
    @args
exit $LASTEXITCODE
