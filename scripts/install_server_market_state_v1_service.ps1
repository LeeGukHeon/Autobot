param(
    [string]$ProjectRoot = "",
    [string]$PythonExe = "",
    [string]$ServiceUser = "ubuntu",
    [string]$ServiceUnitName = "autobot-market-state-v1.service",
    [string]$TimerUnitName = "autobot-market-state-v1.timer",
    [string]$RefreshScript = "",
    [string]$Quote = "KRW",
    [int]$DaysAgo = 1,
    [string]$OnCalendar = "*-*-* 01:00:00",
    [string]$LockFile = "/tmp/autobot-market-state-v1.lock",
    [switch]$NoStart,
    [switch]$NoEnable,
    [switch]$DryRun
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

. (Join-Path $PSScriptRoot "systemd_service_utils.ps1")

function Resolve-DefaultRefreshScript {
    param([string]$Root)
    return (Join-Path $Root "scripts/run_market_state_v1_refresh.ps1")
}

$resolvedProjectRoot = if ([string]::IsNullOrWhiteSpace($ProjectRoot)) { Resolve-DefaultProjectRoot } else { $ProjectRoot }
$resolvedProjectRoot = [System.IO.Path]::GetFullPath($resolvedProjectRoot)
$resolvedPythonExe = if ([string]::IsNullOrWhiteSpace($PythonExe)) { Resolve-DefaultPythonExe -Root $resolvedProjectRoot } else { $PythonExe }
$resolvedRefreshScript = if ([string]::IsNullOrWhiteSpace($RefreshScript)) { Resolve-DefaultRefreshScript -Root $resolvedProjectRoot } else { $RefreshScript }
$resolvedPwshExe = Resolve-PwshExe

$refreshArgs = @(
    "-NoProfile",
    "-ExecutionPolicy", "Bypass",
    "-File", $resolvedRefreshScript,
    "-ProjectRoot", $resolvedProjectRoot,
    "-PythonExe", $resolvedPythonExe,
    "-Quote", $Quote,
    "-DaysAgo", ([string]([Math]::Max([int]$DaysAgo, 1)))
)
$refreshCommand = $resolvedPwshExe + " " + (($refreshArgs | ForEach-Object { Quote-ShellArg ([string]$_) }) -join " ")
$lockCommand = "if command -v flock >/dev/null 2>&1; then exec flock -n " + (Quote-ShellArg $LockFile) + " bash -lc " + (Quote-ShellArg $refreshCommand) + "; else exec bash -lc " + (Quote-ShellArg $refreshCommand) + "; fi"
$execStart = "/bin/bash -lc " + (Quote-ShellArg $lockCommand)

$serviceContent = @"
[Unit]
Description=Autobot market_state_v1 daily refresh
After=network-online.target
Wants=network-online.target

[Service]
Type=oneshot
User=$ServiceUser
WorkingDirectory=$resolvedProjectRoot
Environment=PYTHONUNBUFFERED=1
ExecStart=$execStart
TimeoutStartSec=7200
StandardOutput=journal
StandardError=journal
"@

$timerContent = @"
[Unit]
Description=Autobot market_state_v1 daily refresh timer

[Timer]
OnCalendar=$OnCalendar
Persistent=true
Unit=$ServiceUnitName

[Install]
WantedBy=timers.target
"@

if ($DryRun) {
    Write-Host ("[market-state-v1-install][dry-run] service={0}" -f $ServiceUnitName)
    Write-Host ("[market-state-v1-install][dry-run] refresh_script={0}" -f $resolvedRefreshScript)
    Write-Host $serviceContent
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

& systemctl cat $ServiceUnitName
& systemctl cat $TimerUnitName
