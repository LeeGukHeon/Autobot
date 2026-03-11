param(
    [string]$ProjectRoot = "",
    [string]$PythonExe = "",
    [string]$WrapperScript = "",
    [string]$AcceptanceScript = "",
    [string]$ServiceUnitName = "autobot-v4-rank-shadow.service",
    [string]$TimerUnitName = "autobot-v4-rank-shadow.timer",
    [string]$OnCalendar = "*-*-* 04:40:00",
    [string]$Description = "Autobot V4 Rank Shadow Acceptance",
    [string]$ServiceUser = "ubuntu",
    [string[]]$BlockOnActiveUnits = @(
        "autobot-v4-challenger-spawn.service",
        "autobot-v4-challenger-promote.service"
    ),
    [string[]]$AcceptanceArgs = @("-SkipPaperSoak"),
    [bool]$SkipDailyPipeline = $true,
    [bool]$SkipReportRefresh = $true,
    [switch]$NoStart,
    [switch]$NoEnable,
    [switch]$DryRun
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

. (Join-Path $PSScriptRoot "systemd_service_utils.ps1")

function Resolve-DefaultWrapperScript {
    param([string]$Root)
    return (Join-Path $Root "scripts/daily_rank_shadow_cycle_for_server.ps1")
}

function Resolve-DefaultAcceptanceScript {
    param([string]$Root)
    return (Join-Path $Root "scripts/v4_rank_shadow_candidate_acceptance.ps1")
}

$resolvedProjectRoot = if ([string]::IsNullOrWhiteSpace($ProjectRoot)) { Resolve-DefaultProjectRoot } else { $ProjectRoot }
$resolvedProjectRoot = [System.IO.Path]::GetFullPath($resolvedProjectRoot)
$resolvedPythonExe = if ([string]::IsNullOrWhiteSpace($PythonExe)) { Resolve-DefaultPythonExe -Root $resolvedProjectRoot } else { $PythonExe }
$resolvedWrapperScript = if ([string]::IsNullOrWhiteSpace($WrapperScript)) { Resolve-DefaultWrapperScript -Root $resolvedProjectRoot } else { $WrapperScript }
$resolvedAcceptanceScript = if ([string]::IsNullOrWhiteSpace($AcceptanceScript)) { Resolve-DefaultAcceptanceScript -Root $resolvedProjectRoot } else { $AcceptanceScript }
$resolvedPwshExe = Resolve-PwshExe

$wrapperArgList = @(
    "-NoProfile",
    "-ExecutionPolicy", "Bypass",
    "-File", $resolvedWrapperScript,
    "-ProjectRoot", $resolvedProjectRoot,
    "-PythonExe", $resolvedPythonExe,
    "-AcceptanceScript", $resolvedAcceptanceScript
)
if ($SkipDailyPipeline) {
    $wrapperArgList += "-SkipDailyPipeline"
}
if ($SkipReportRefresh) {
    $wrapperArgList += "-SkipReportRefresh"
}
if (@($BlockOnActiveUnits).Count -gt 0) {
    $wrapperArgList += "-BlockOnActiveUnits"
    $wrapperArgList += @($BlockOnActiveUnits)
}
if (@($AcceptanceArgs).Count -gt 0) {
    $wrapperArgList += "-AcceptanceArgs"
    $wrapperArgList += @($AcceptanceArgs)
}

$execStartCommand = $resolvedPwshExe + " " + (($wrapperArgList | ForEach-Object { Quote-ShellArg ([string]$_) }) -join " ")
$execStart = "/bin/bash -lc " + (Quote-ShellArg $execStartCommand)

$serviceContent = @"
[Unit]
Description=$Description
After=network-online.target
Wants=network-online.target

[Service]
Type=oneshot
User=$ServiceUser
WorkingDirectory=$resolvedProjectRoot
Environment=PYTHONUNBUFFERED=1
ExecStart=$execStart
TimeoutStartSec=infinity
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
"@

$timerContent = @"
[Unit]
Description=$Description Timer

[Timer]
OnCalendar=$OnCalendar
Persistent=true
Unit=$ServiceUnitName

[Install]
WantedBy=timers.target
"@

if ($DryRun) {
    Write-Host ("[rank-shadow-install][dry-run] service={0}" -f $ServiceUnitName)
    Write-Host ("[rank-shadow-install][dry-run] service_user={0}" -f $ServiceUser)
    Write-Host ("[rank-shadow-install][dry-run] pwsh={0}" -f $resolvedPwshExe)
    Write-Host $serviceContent
    Write-Host ("[rank-shadow-install][dry-run] timer={0}" -f $TimerUnitName)
    Write-Host $timerContent
    exit 0
}

Enable-UserLinger -UserName $ServiceUser
Install-UnitFile -UnitName $ServiceUnitName -Content $serviceContent
Install-UnitFile -UnitName $TimerUnitName -Content $timerContent

& sudo systemctl daemon-reload
if ($LASTEXITCODE -ne 0) {
    throw "systemctl daemon-reload failed"
}
if (-not $NoEnable) {
    & sudo systemctl enable $TimerUnitName
    if ($LASTEXITCODE -ne 0) {
        throw "systemctl enable failed: $TimerUnitName"
    }
}
if (-not $NoStart) {
    & sudo systemctl restart $TimerUnitName
    if ($LASTEXITCODE -ne 0) {
        throw "systemctl restart failed: $TimerUnitName"
    }
}

& systemctl status $TimerUnitName --no-pager
