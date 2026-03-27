param(
    [string]$ProjectRoot = "",
    [string]$PythonExe = "",
    [string]$WrapperScript = "",
    [string]$TargetServiceName = "autobot-daily-micro.service",
    [string]$TargetTimerName = "autobot-daily-micro.timer",
    [string]$DisableTimerName = "autobot-daily-v4-accept.timer",
    [string]$ServiceUser = "ubuntu",
    [string]$ChampionCompareModelFamily = "",
    [string]$ChampionUnitName = "autobot-paper-v4.service",
    [string]$ChallengerUnitName = "autobot-paper-v4-challenger.service",
    [string[]]$PromotionTargetUnits = @(),
    [string[]]$CandidateTargetUnits = @(),
    [switch]$NoRestartTimer,
    [switch]$DryRun
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

. (Join-Path $PSScriptRoot "systemd_service_utils.ps1")

function Resolve-DefaultWrapperScript {
    param([string]$Root)
    return (Join-Path $Root "scripts/daily_champion_challenger_v4_for_server.ps1")
}

$resolvedProjectRoot = if ([string]::IsNullOrWhiteSpace($ProjectRoot)) { Resolve-DefaultProjectRoot } else { $ProjectRoot }
$resolvedProjectRoot = [System.IO.Path]::GetFullPath($resolvedProjectRoot)
$resolvedPythonExe = if ([string]::IsNullOrWhiteSpace($PythonExe)) { Resolve-DefaultPythonExe -Root $resolvedProjectRoot } else { $PythonExe }
$resolvedWrapperScript = if ([string]::IsNullOrWhiteSpace($WrapperScript)) { Resolve-DefaultWrapperScript -Root $resolvedProjectRoot } else { $WrapperScript }
$resolvedPwshExe = Resolve-PwshExe

$wrapperArgList = @(
    "-NoProfile",
    "-ExecutionPolicy", "Bypass",
    "-File", $resolvedWrapperScript,
    "-ProjectRoot", $resolvedProjectRoot,
    "-PythonExe", $resolvedPythonExe,
    "-ChampionUnitName", $ChampionUnitName,
    "-ChallengerUnitName", $ChallengerUnitName
)
if (-not [string]::IsNullOrWhiteSpace($ChampionCompareModelFamily)) {
    $wrapperArgList += @("-ChampionCompareModelFamily", $ChampionCompareModelFamily)
}
if (@($PromotionTargetUnits).Count -gt 0) {
    $wrapperArgList += "-PromotionTargetUnits"
    $wrapperArgList += (Join-DelimitedStringArray -Values $PromotionTargetUnits)
}
if (@($CandidateTargetUnits).Count -gt 0) {
    $wrapperArgList += "-CandidateTargetUnits"
    $wrapperArgList += (Join-DelimitedStringArray -Values $CandidateTargetUnits)
}
$execStartCommand = $resolvedPwshExe + " " + (($wrapperArgList | ForEach-Object { Quote-ShellArg ([string]$_) }) -join " ")
$execStart = "/bin/bash -lc " + (Quote-ShellArg $execStartCommand)

$overrideContent = @"
[Service]
TimeoutStartSec=0
User=$ServiceUser
ExecStart=
ExecStart=$execStart
"@

if ($DryRun) {
    Write-Host ("[daily-parallel-install][dry-run] target_service={0}" -f $TargetServiceName)
    Write-Host ("[daily-parallel-install][dry-run] service_user={0}" -f $ServiceUser)
    Write-Host ("[daily-parallel-install][dry-run] pwsh={0}" -f $resolvedPwshExe)
    Write-Host $overrideContent
    Write-Host ("[daily-parallel-install][dry-run] target_timer={0}" -f $TargetTimerName)
    if (-not [string]::IsNullOrWhiteSpace($DisableTimerName)) {
        Write-Host ("[daily-parallel-install][dry-run] disable_timer={0}" -f $DisableTimerName)
    }
    exit 0
}

Enable-UserLinger -UserName $ServiceUser
Install-DropInFile -UnitName $TargetServiceName -DropInName "override.conf" -Content $overrideContent

& sudo systemctl daemon-reload
if ($LASTEXITCODE -ne 0) {
    throw "systemctl daemon-reload failed"
}

if (-not [string]::IsNullOrWhiteSpace($DisableTimerName)) {
    & sudo systemctl disable --now $DisableTimerName 2>$null
}

if (-not $NoRestartTimer) {
    & sudo systemctl restart $TargetTimerName
    if ($LASTEXITCODE -ne 0) {
        throw "systemctl restart failed: $TargetTimerName"
    }
}

& systemctl cat $TargetServiceName
