param(
    [string]$ProjectRoot = "",
    [string]$PythonExe = "",
    [string]$ServiceUser = "ubuntu",
    [string]$ServiceUnitName = "autobot-raw-trade-v1.service",
    [string]$TimerUnitName = "autobot-raw-trade-v1.timer",
    [string]$RefreshScript = "",
    [string]$Quote = "KRW",
    [int]$WindowDays = 2,
    [string]$OnBootSec = "6min",
    [string]$OnUnitActiveSec = "20min",
    [string]$LockFile = "/tmp/autobot-raw-trade-v1.lock",
    [switch]$NoStart,
    [switch]$NoEnable,
    [switch]$DryRun
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

. (Join-Path $PSScriptRoot "systemd_service_utils.ps1")

function Resolve-DefaultRefreshScript {
    param([string]$Root)
    return (Join-Path $Root "scripts/run_raw_trade_v1_refresh.ps1")
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
    "-WindowDays", ([string]([Math]::Max([int]$WindowDays, 1)))
)
$refreshCommand = $resolvedPwshExe + " " + (($refreshArgs | ForEach-Object { Quote-ShellArg ([string]$_) }) -join " ")
$lockCommand = "if command -v flock >/dev/null 2>&1; then exec flock -n " + (Quote-ShellArg $LockFile) + " bash -lc " + (Quote-ShellArg $refreshCommand) + "; else exec bash -lc " + (Quote-ShellArg $refreshCommand) + "; fi"
$execStart = "/bin/bash -lc " + (Quote-ShellArg $lockCommand)

$serviceContent = @"
[Unit]
Description=Autobot canonical raw_trade_v1 refresh
After=network-online.target
Wants=network-online.target

[Service]
Type=oneshot
User=$ServiceUser
WorkingDirectory=$resolvedProjectRoot
Environment=PYTHONUNBUFFERED=1
ExecStart=$execStart
TimeoutStartSec=1800
StandardOutput=journal
StandardError=journal
"@

$timerContent = @"
[Unit]
Description=Autobot canonical raw_trade_v1 refresh timer

[Timer]
OnActiveSec=$OnBootSec
OnBootSec=$OnBootSec
OnUnitActiveSec=$OnUnitActiveSec
Persistent=true
Unit=$ServiceUnitName

[Install]
WantedBy=timers.target
"@

if ($DryRun) {
    Write-Host ("[raw-trade-v1-install][dry-run] service={0}" -f $ServiceUnitName)
    Write-Host ("[raw-trade-v1-install][dry-run] refresh_script={0}" -f $resolvedRefreshScript)
    Write-Host ("[raw-trade-v1-install][dry-run] lock_file={0}" -f $LockFile)
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
