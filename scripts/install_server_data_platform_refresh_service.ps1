param(
    [string]$ProjectRoot = "",
    [string]$PythonExe = "",
    [string]$ServiceUnitName = "autobot-data-platform-refresh.service",
    [string]$TimerUnitName = "autobot-data-platform-refresh.timer",
    [string]$ServiceUser = "ubuntu",
    [string]$RefreshScript = "",
    [string]$Quote = "KRW",
    [string]$MarketMode = "top_n_by_recent_value_est",
    [int]$TopN = 50,
    [int]$SecondMaxBackfillDays = 7,
    [int]$CandlesMaxRequests = 120,
    [int]$WsCandleDurationSec = 180,
    [int]$Lob30DurationSec = 180,
    [int]$TensorMaxMarkets = 20,
    [int]$TensorMaxAnchorsPerMarket = 64,
    [int]$TensorRecentDates = 2,
    [string[]]$TensorMarkets = @(),
    [string]$OnBootSec = "5min",
    [string]$OnUnitActiveSec = "20min",
    [switch]$NoStart,
    [switch]$NoEnable,
    [switch]$DryRun
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

. (Join-Path $PSScriptRoot "systemd_service_utils.ps1")

function Resolve-DefaultRefreshScript {
    param([string]$Root)
    return (Join-Path $Root "scripts/refresh_data_platform_layers.ps1")
}

$resolvedProjectRoot = if ([string]::IsNullOrWhiteSpace($ProjectRoot)) { Resolve-DefaultProjectRoot } else { $ProjectRoot }
$resolvedProjectRoot = [System.IO.Path]::GetFullPath($resolvedProjectRoot)
$resolvedPythonExe = if ([string]::IsNullOrWhiteSpace($PythonExe)) { Resolve-DefaultPythonExe -Root $resolvedProjectRoot } else { $PythonExe }
$resolvedRefreshScript = if ([string]::IsNullOrWhiteSpace($RefreshScript)) { Resolve-DefaultRefreshScript -Root $resolvedProjectRoot } else { $RefreshScript }
$resolvedPwshExe = Resolve-PwshExe
$serializedTensorMarkets = Join-DelimitedStringArray -Values $TensorMarkets

$refreshArgs = @(
    "-NoProfile",
    "-ExecutionPolicy", "Bypass",
    "-File", $resolvedRefreshScript,
    "-ProjectRoot", $resolvedProjectRoot,
    "-PythonExe", $resolvedPythonExe,
    "-Quote", $Quote,
    "-MarketMode", $MarketMode,
    "-TopN", ([string]([Math]::Max([int]$TopN, 1))),
    "-SecondMaxBackfillDays", ([string]([Math]::Max([int]$SecondMaxBackfillDays, 1))),
    "-CandlesMaxRequests", ([string]([Math]::Max([int]$CandlesMaxRequests, 1))),
    "-WsCandleDurationSec", ([string]([Math]::Max([int]$WsCandleDurationSec, 1))),
    "-Lob30DurationSec", ([string]([Math]::Max([int]$Lob30DurationSec, 1))),
    "-TensorMaxMarkets", ([string]([Math]::Max([int]$TensorMaxMarkets, 1))),
    "-TensorMaxAnchorsPerMarket", ([string]([Math]::Max([int]$TensorMaxAnchorsPerMarket, 1))),
    "-TensorRecentDates", ([string]([Math]::Max([int]$TensorRecentDates, 1)))
)
if (-not [string]::IsNullOrWhiteSpace($serializedTensorMarkets)) {
    $refreshArgs += @("-TensorMarkets", $serializedTensorMarkets)
}
$execStartCommand = $resolvedPwshExe + " " + (($refreshArgs | ForEach-Object { Quote-ShellArg ([string]$_) }) -join " ")
$execStart = "/bin/bash -lc " + (Quote-ShellArg $execStartCommand)

$serviceContent = @"
[Unit]
Description=Autobot Data Platform Refresh
After=network-online.target
Wants=network-online.target

[Service]
Type=oneshot
User=$ServiceUser
WorkingDirectory=$resolvedProjectRoot
Environment=PYTHONUNBUFFERED=1
ExecStart=$execStart
TimeoutStartSec=2700
StandardOutput=journal
StandardError=journal
"@

$timerContent = @"
[Unit]
Description=Autobot Data Platform Refresh Timer

[Timer]
OnBootSec=$OnBootSec
OnUnitActiveSec=$OnUnitActiveSec
Persistent=true
Unit=$ServiceUnitName

[Install]
WantedBy=timers.target
"@

if ($DryRun) {
    Write-Host ("[data-platform-install][dry-run] service={0}" -f $ServiceUnitName)
    Write-Host ("[data-platform-install][dry-run] refresh_script={0}" -f $resolvedRefreshScript)
    Write-Host "[data-platform-install][dry-run] datasets=candles_second_v1,ws_candle_v1,lob30_v1,sequence_v1"
    Write-Host ("[data-platform-install][dry-run] tensor_recent_dates={0}" -f ([Math]::Max([int]$TensorRecentDates, 1)))
    Write-Host $serviceContent
    Write-Host ("[data-platform-install][dry-run] timer={0}" -f $TimerUnitName)
    Write-Host $timerContent
    Write-Host ("[data-platform-install][dry-run] tensor_markets={0}" -f $serializedTensorMarkets)
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
    & sudo systemctl start $ServiceUnitName
    if ($LASTEXITCODE -ne 0) {
        throw "systemctl start failed: $ServiceUnitName"
    }
}

& systemctl cat $ServiceUnitName
& systemctl cat $TimerUnitName
