param(
    [string]$ProjectRoot = "",
    [string]$PythonExe = "",
    [string]$ServiceUser = "ubuntu",
    [string]$ServiceUnitName = "autobot-storage-retention.service",
    [string]$TimerUnitName = "autobot-storage-retention.timer",
    [string]$OnCalendar = "*-*-* 06:30:00",
    [string]$ModelFamily = "train_v4_crypto_cs",
    [double]$WarningThresholdGb = 100.0,
    [double]$ForceThresholdGb = 120.0,
    [int]$WsPublicRetentionDays = 14,
    [int]$RawTicksRetentionDays = 30,
    [int]$MicroParquetRetentionDays = 90,
    [int]$CandlesApiRetentionDays = 90,
    [int]$PaperRunsRetentionDays = 30,
    [int]$BacktestRunsRetentionDays = 1,
    [int]$ExecutionBacktestRetentionDays = 1,
    [int]$RegistryRetentionDays = 30,
    [int]$RegistryKeepRecentCount = 6,
    [int]$EmergencyPaperRunsRetentionDays = 7,
    [int]$EmergencyBacktestRunsRetentionDays = 1,
    [int]$EmergencyExecutionBacktestRetentionDays = 1,
    [int]$EmergencyRegistryRetentionDays = 14,
    [int]$EmergencyRegistryKeepRecentCount = 3,
    [switch]$NoStart,
    [switch]$NoEnable,
    [switch]$DryRun
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

. (Join-Path $PSScriptRoot "systemd_service_utils.ps1")

function New-ServiceContent {
    param(
        [string]$Description,
        [string]$UserName,
        [string]$WorkingRoot,
        [string]$ExecStart
    )
    return @"
[Unit]
Description=$Description
After=network-online.target
Wants=network-online.target

[Service]
Type=oneshot
User=$UserName
WorkingDirectory=$WorkingRoot
Environment=PYTHONUNBUFFERED=1
ExecStart=$ExecStart
TimeoutStartSec=infinity
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
"@
}

function New-TimerContent {
    param(
        [string]$Description,
        [string]$OnCalendarValue,
        [string]$UnitName
    )
    return @"
[Unit]
Description=$Description Timer

[Timer]
OnCalendar=$OnCalendarValue
Persistent=true
Unit=$UnitName

[Install]
WantedBy=timers.target
"@
}

function Build-ExecStart {
    param(
        [string]$PythonPath,
        [string]$Root,
        [string]$Family
    )
    $argList = @(
        "-m", "autobot.ops.storage_retention",
        "--project-root", $Root,
        "--model-family", $Family,
        "--ws-public-retention-days", ([string]([Math]::Max([int]$WsPublicRetentionDays, 1))),
        "--raw-ticks-retention-days", ([string]([Math]::Max([int]$RawTicksRetentionDays, 1))),
        "--micro-parquet-retention-days", ([string]([Math]::Max([int]$MicroParquetRetentionDays, 1))),
        "--candles-api-retention-days", ([string]([Math]::Max([int]$CandlesApiRetentionDays, 1))),
        "--paper-runs-retention-days", ([string]([Math]::Max([int]$PaperRunsRetentionDays, 1))),
        "--backtest-runs-retention-days", ([string]([Math]::Max([int]$BacktestRunsRetentionDays, 1))),
        "--execution-backtest-retention-days", ([string]([Math]::Max([int]$ExecutionBacktestRetentionDays, 1))),
        "--registry-retention-days", ([string]([Math]::Max([int]$RegistryRetentionDays, 1))),
        "--registry-keep-recent-count", ([string]([Math]::Max([int]$RegistryKeepRecentCount, 0))),
        "--emergency-paper-runs-retention-days", ([string]([Math]::Max([int]$EmergencyPaperRunsRetentionDays, 1))),
        "--emergency-backtest-runs-retention-days", ([string]([Math]::Max([int]$EmergencyBacktestRunsRetentionDays, 1))),
        "--emergency-execution-backtest-retention-days", ([string]([Math]::Max([int]$EmergencyExecutionBacktestRetentionDays, 1))),
        "--emergency-registry-retention-days", ([string]([Math]::Max([int]$EmergencyRegistryRetentionDays, 1))),
        "--emergency-registry-keep-recent-count", ([string]([Math]::Max([int]$EmergencyRegistryKeepRecentCount, 0))),
        "--warning-threshold-gb", ([string][double]$WarningThresholdGb),
        "--force-threshold-gb", ([string][double]$ForceThresholdGb),
        "--compact-candles-api", "true"
    )
    $command = $PythonPath + " " + (($argList | ForEach-Object { Quote-ShellArg ([string]$_) }) -join " ")
    return ("/bin/bash -lc " + (Quote-ShellArg $command))
}

$resolvedProjectRoot = if ([string]::IsNullOrWhiteSpace($ProjectRoot)) { Resolve-DefaultProjectRoot } else { $ProjectRoot }
$resolvedProjectRoot = [System.IO.Path]::GetFullPath($resolvedProjectRoot)
$resolvedPythonExe = if ([string]::IsNullOrWhiteSpace($PythonExe)) { Resolve-DefaultPythonExe -Root $resolvedProjectRoot } else { $PythonExe }

$execStart = Build-ExecStart -PythonPath $resolvedPythonExe -Root $resolvedProjectRoot -Family $ModelFamily
$serviceContent = New-ServiceContent `
    -Description "Autobot Storage Retention Cleanup" `
    -UserName $ServiceUser `
    -WorkingRoot $resolvedProjectRoot `
    -ExecStart $execStart
$timerContent = New-TimerContent `
    -Description "Autobot Storage Retention Cleanup" `
    -OnCalendarValue $OnCalendar `
    -UnitName $ServiceUnitName

if ($DryRun) {
    Write-Host ("[storage-retention-install][dry-run] service_user={0}" -f $ServiceUser)
    Write-Host ("[storage-retention-install][dry-run] service={0}" -f $ServiceUnitName)
    Write-Host $serviceContent
    Write-Host ("[storage-retention-install][dry-run] timer={0}" -f $TimerUnitName)
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
