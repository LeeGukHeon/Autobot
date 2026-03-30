param(
    [string]$ProjectRoot = "",
    [string]$PythonExe = "",
    [string]$ServiceUser = "ubuntu",
    [string]$ServiceUnitName = "autobot-raw-ticks-daily.service",
    [string]$TimerUnitName = "autobot-raw-ticks-daily.timer",
    [string]$RefreshScript = "",
    [string]$Quote = "KRW",
    [int]$TopN = 50,
    [int]$DaysAgo = 1,
    [int]$Workers = 1,
    [int]$MaxPagesPerTarget = 50,
    [string]$OnBootSec = "10min",
    [string]$OnUnitActiveSec = "6h",
    [string]$LockFile = "/tmp/autobot-raw-ticks-daily.lock",
    [switch]$NoStart,
    [switch]$NoEnable,
    [switch]$DryRun
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

. (Join-Path $PSScriptRoot "systemd_service_utils.ps1")

function Resolve-DefaultRefreshScript {
    param([string]$Root)
    return (Join-Path $Root "scripts/run_raw_ticks_daily.ps1")
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
    "-TopN", ([string]([Math]::Max([int]$TopN, 1))),
    "-DaysAgo", ([string]([Math]::Max([int]$DaysAgo, 1))),
    "-Workers", ([string]([Math]::Max([int]$Workers, 1))),
    "-MaxPagesPerTarget", ([string]([Math]::Max([int]$MaxPagesPerTarget, 1)))
)
$refreshCommand = $resolvedPwshExe + " " + (($refreshArgs | ForEach-Object { Quote-ShellArg ([string]$_) }) -join " ")
$lockCommand = "if command -v flock >/dev/null 2>&1; then exec flock -n " + (Quote-ShellArg $LockFile) + " bash -lc " + (Quote-ShellArg $refreshCommand) + "; else exec bash -lc " + (Quote-ShellArg $refreshCommand) + "; fi"
$execStart = "/bin/bash -lc " + (Quote-ShellArg $lockCommand)

$serviceContent = @"
[Unit]
Description=Autobot raw ticks daily refresh
After=network-online.target
Wants=network-online.target

[Service]
Type=oneshot
User=$ServiceUser
WorkingDirectory=$resolvedProjectRoot
Environment=PYTHONUNBUFFERED=1
ExecStart=$execStart
TimeoutStartSec=3600
StandardOutput=journal
StandardError=journal
"@

$timerContent = @"
[Unit]
Description=Autobot raw ticks daily refresh timer

[Timer]
OnBootSec=$OnBootSec
OnUnitActiveSec=$OnUnitActiveSec
Persistent=true
Unit=$ServiceUnitName

[Install]
WantedBy=timers.target
"@

if ($DryRun) {
    Write-Host ("[raw-ticks-install][dry-run] service={0}" -f $ServiceUnitName)
    Write-Host ("[raw-ticks-install][dry-run] refresh_script={0}" -f $resolvedRefreshScript)
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
