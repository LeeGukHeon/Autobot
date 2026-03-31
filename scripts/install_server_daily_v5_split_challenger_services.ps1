param(
    [string]$ProjectRoot = "",
    [string]$PythonExe = "",
    [string]$WrapperScript = "",
    [string]$AcceptanceScript = "",
    [string]$RuntimeInstallScript = "",
    [string]$CandidateAdoptionScript = "",
    [string]$ServiceUser = "ubuntu",
    [string]$ModelFamily = "train_v5_fusion",
    [string]$ChampionCompareModelFamily = "",
    [string]$PairedPaperModelFamily = "",
    [string]$ChampionUnitName = "autobot-paper-v5.service",
    [string[]]$PromotionTargetUnits = @("autobot-live-alpha.service"),
    [string[]]$CandidateTargetUnits = @("autobot-live-alpha-canary.service"),
    [string]$PromoteServiceUnitName = "autobot-v5-challenger-promote.service",
    [string]$PromoteTimerUnitName = "autobot-v5-challenger-promote.timer",
    [string]$PromoteOnCalendar = "*-*-* 00:10:00",
    [string]$SpawnServiceUnitName = "autobot-v5-challenger-spawn.service",
    [string]$SpawnTimerUnitName = "autobot-v5-challenger-spawn.timer",
    [string]$SpawnOnCalendar = "*-*-* 00:20:00",
    [string]$LockFile = "/tmp/autobot-train-acceptance.lock",
    [switch]$NoStart,
    [switch]$NoEnable,
    [switch]$DryRun
)

$scriptPath = Join-Path $PSScriptRoot "install_server_daily_split_challenger_services.ps1"
$resolvedWrapperScript = if ([string]::IsNullOrWhiteSpace($WrapperScript)) {
    (Join-Path $PSScriptRoot "daily_champion_challenger_v5_for_server.ps1")
} else {
    $WrapperScript
}
$resolvedCandidateAdoptionScript = if ([string]::IsNullOrWhiteSpace($CandidateAdoptionScript)) {
    (Join-Path $PSScriptRoot "adopt_v5_candidate_for_server.ps1")
} else {
    $CandidateAdoptionScript
}
& $scriptPath `
    -ProjectRoot $ProjectRoot `
    -PythonExe $PythonExe `
    -WrapperScript $resolvedWrapperScript `
    -AcceptanceScript $AcceptanceScript `
    -RuntimeInstallScript $RuntimeInstallScript `
    -CandidateAdoptionScript $resolvedCandidateAdoptionScript `
    -ServiceUser $ServiceUser `
    -ModelFamily $ModelFamily `
    -ChampionCompareModelFamily $ChampionCompareModelFamily `
    -PairedPaperModelFamily $PairedPaperModelFamily `
    -ChampionUnitName $ChampionUnitName `
    -ChallengerUnitName "" `
    -PromotionTargetUnits $PromotionTargetUnits `
    -CandidateTargetUnits $CandidateTargetUnits `
    -PromoteServiceUnitName $PromoteServiceUnitName `
    -PromoteTimerUnitName $PromoteTimerUnitName `
    -PromoteOnCalendar $PromoteOnCalendar `
    -SpawnServiceUnitName $SpawnServiceUnitName `
    -SpawnTimerUnitName $SpawnTimerUnitName `
    -SpawnOnCalendar $SpawnOnCalendar `
    -LockFile $LockFile `
    -DisableLegacyTimerNames @("autobot-v4-challenger-spawn.timer", "autobot-v4-challenger-promote.timer", "autobot-daily-micro.timer", "autobot-daily-v4-accept.timer") `
    -DisableLegacyServiceNames @("autobot-v4-challenger-spawn.service", "autobot-v4-challenger-promote.service", "autobot-daily-micro.service", "autobot-daily-v4-accept.service", "autobot-paper-v4-replay.service", "autobot-live-alpha-replay-shadow.service") `
    -NoStart:$NoStart `
    -NoEnable:$NoEnable `
    -DryRun:$DryRun `
    @args
exit $LASTEXITCODE
