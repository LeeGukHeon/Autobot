param(
    [string]$ProjectRoot = "",
    [string]$PythonExe = "",
    [string]$ServiceUnitName = "autobot-live-execution-policy.service",
    [string]$TimerUnitName = "autobot-live-execution-policy.timer",
    [string]$ServiceUser = "ubuntu",
    [string]$RefreshScript = "",
    [string]$OutputDir = "logs/live_execution_policy",
    [string[]]$StateDbPaths = @(
        "data/state/live_state.db",
        "data/state/live_canary/live_state.db",
        "data/state/live_candidate/live_state.db"
    ),
    [string]$OnCalendar = "*-*-* 23:40:00",
    [int]$LookbackDays = 14,
    [int]$Limit = 5000,
    [switch]$NoStart,
    [switch]$NoEnable,
    [switch]$DryRun
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

. (Join-Path $PSScriptRoot "systemd_service_utils.ps1")

function Resolve-DefaultRefreshScript {
    param([string]$Root)
    return (Join-Path $Root "scripts/refresh_live_execution_policy.ps1")
}

$resolvedProjectRoot = if ([string]::IsNullOrWhiteSpace($ProjectRoot)) { Resolve-DefaultProjectRoot } else { $ProjectRoot }
$resolvedProjectRoot = [System.IO.Path]::GetFullPath($resolvedProjectRoot)
$resolvedPythonExe = if ([string]::IsNullOrWhiteSpace($PythonExe)) { Resolve-DefaultPythonExe -Root $resolvedProjectRoot } else { $PythonExe }
$resolvedRefreshScript = if ([string]::IsNullOrWhiteSpace($RefreshScript)) { Resolve-DefaultRefreshScript -Root $resolvedProjectRoot } else { $RefreshScript }
$resolvedPwshExe = Resolve-PwshExe
$serializedStateDbPaths = Join-DelimitedStringArray -Values $StateDbPaths

$refreshArgs = @(
    "-NoProfile",
    "-ExecutionPolicy", "Bypass",
    "-File", $resolvedRefreshScript,
    "-ProjectRoot", $resolvedProjectRoot,
    "-PythonExe", $resolvedPythonExe,
    "-OutputDir", $OutputDir,
    "-StateDbPaths", $serializedStateDbPaths,
    "-LookbackDays", [string]([Math]::Max($LookbackDays, 1)),
    "-Limit", [string]([Math]::Max($Limit, 1))
)
$execStartCommand = $resolvedPwshExe + " " + (($refreshArgs | ForEach-Object { Quote-ShellArg ([string]$_) }) -join " ")
$execStart = "/bin/bash -lc " + (Quote-ShellArg $execStartCommand)

$serviceContent = @"
[Unit]
Description=Autobot Live Execution Policy Refresh
After=network-online.target
Wants=network-online.target

[Service]
Type=oneshot
User=$ServiceUser
WorkingDirectory=$resolvedProjectRoot
Environment=PYTHONUNBUFFERED=1
ExecStart=$execStart
StandardOutput=journal
StandardError=journal
"@

$timerContent = @"
[Unit]
Description=Autobot Live Execution Policy Refresh Timer

[Timer]
OnCalendar=$OnCalendar
Persistent=true
Unit=$ServiceUnitName

[Install]
WantedBy=timers.target
"@

if ($DryRun) {
    Write-Host ("[live-exec-install][dry-run] service={0}" -f $ServiceUnitName)
    Write-Host $serviceContent
    Write-Host ("[live-exec-install][dry-run] timer={0}" -f $TimerUnitName)
    Write-Host $timerContent
    Write-Host ("[live-exec-install][dry-run] state_db_paths={0}" -f $serializedStateDbPaths)
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
