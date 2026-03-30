param(
    [string]$ProjectRoot = "",
    [string]$PythonExe = "",
    [string]$ServiceUser = "ubuntu",
    [string]$ServiceUnitName = "autobot-candles-api-refresh.service",
    [string]$TimerUnitName = "autobot-candles-api-refresh.timer",
    [string]$RefreshScript = "",
    [string]$Quote = "KRW",
    [string]$MarketMode = "top_n_by_recent_value_est",
    [int]$TopN = 50,
    [int]$LookbackMonths = 3,
    [string]$Tf = "1m,5m,15m,60m,240m",
    [int]$MaxBackfillDays1m = 3,
    [int]$Workers = 1,
    [int]$MaxRequests = 120,
    [string]$OnBootSec = "4min",
    [string]$OnUnitActiveSec = "20min",
    [string]$LockFile = "/tmp/autobot-candles-api-refresh.lock",
    [switch]$NoStart,
    [switch]$NoEnable,
    [switch]$DryRun
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

. (Join-Path $PSScriptRoot "systemd_service_utils.ps1")

function Resolve-DefaultRefreshScript {
    param([string]$Root)
    return (Join-Path $Root "scripts/run_candles_api_refresh.ps1")
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
    "-MarketMode", $MarketMode,
    "-TopN", ([string]([Math]::Max([int]$TopN, 1))),
    "-LookbackMonths", ([string]([Math]::Max([int]$LookbackMonths, 1))),
    "-Tf", $Tf,
    "-MaxBackfillDays1m", ([string]([Math]::Max([int]$MaxBackfillDays1m, 1))),
    "-Workers", ([string]([Math]::Max([int]$Workers, 1))),
    "-MaxRequests", ([string]([Math]::Max([int]$MaxRequests, 1)))
)
$refreshCommand = $resolvedPwshExe + " " + (($refreshArgs | ForEach-Object { Quote-ShellArg ([string]$_) }) -join " ")
$lockCommand = "if command -v flock >/dev/null 2>&1; then exec flock -n " + (Quote-ShellArg $LockFile) + " bash -lc " + (Quote-ShellArg $refreshCommand) + "; else exec bash -lc " + (Quote-ShellArg $refreshCommand) + "; fi"
$execStart = "/bin/bash -lc " + (Quote-ShellArg $lockCommand)

$serviceContent = @"
[Unit]
Description=Autobot candles_api refresh
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
Description=Autobot candles_api refresh timer

[Timer]
OnBootSec=$OnBootSec
OnUnitActiveSec=$OnUnitActiveSec
Persistent=true
Unit=$ServiceUnitName

[Install]
WantedBy=timers.target
"@

if ($DryRun) {
    Write-Host ("[candles-api-install][dry-run] service={0}" -f $ServiceUnitName)
    Write-Host ("[candles-api-install][dry-run] refresh_script={0}" -f $resolvedRefreshScript)
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
