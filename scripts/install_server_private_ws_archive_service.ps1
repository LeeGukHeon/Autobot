param(
    [string]$ProjectRoot = "",
    [string]$PythonExe = "",
    [string]$ServiceUser = "ubuntu",
    [string]$UnitName = "autobot-private-ws-archive.service",
    [int]$RetentionDays = 30,
    [int]$RotateSec = 3600,
    [int]$DurationSec = 0,
    [switch]$NoStart,
    [switch]$NoEnable,
    [switch]$DryRun
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

. (Join-Path $PSScriptRoot "systemd_service_utils.ps1")

$resolvedProjectRoot = if ([string]::IsNullOrWhiteSpace($ProjectRoot)) { Resolve-DefaultProjectRoot } else { $ProjectRoot }
$resolvedProjectRoot = [System.IO.Path]::GetFullPath($resolvedProjectRoot)
$resolvedPythonExe = if ([string]::IsNullOrWhiteSpace($PythonExe)) { Resolve-DefaultPythonExe -Root $resolvedProjectRoot } else { $PythonExe }

$argList = @(
    "-m", "autobot.cli",
    "collect", "private-ws", "daemon",
    "--raw-root", "data/raw_ws/upbit/private",
    "--meta-dir", "data/raw_ws/upbit/_meta",
    "--retention-days", ([string]([Math]::Max([int]$RetentionDays, 1))),
    "--rotate-sec", ([string]([Math]::Max([int]$RotateSec, 1)))
)
if ([int]$DurationSec -gt 0) {
    $argList += @("--duration-sec", ([string][int]$DurationSec))
}

$command = $resolvedPythonExe + " " + (($argList | ForEach-Object { Quote-ShellArg ([string]$_) }) -join " ")
$activatePath = Join-Path $resolvedProjectRoot ".venv/bin/activate"
$execStart = "/bin/bash -lc " + (Quote-ShellArg ("source " + $activatePath + " && " + $command))

$unitContent = @"
[Unit]
Description=Autobot Private WS Archive
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=$ServiceUser
WorkingDirectory=$resolvedProjectRoot
Environment=PYTHONUNBUFFERED=1
SyslogIdentifier=$($UnitName -replace '\.service$', '')
ExecStart=$execStart
Restart=always
RestartSec=15
TimeoutStopSec=30
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
"@

if ($DryRun) {
    Write-Host ("[private-ws-install][dry-run] service_user={0}" -f $ServiceUser)
    Write-Host ("[private-ws-install][dry-run] unit={0}" -f $UnitName)
    Write-Host $unitContent
    exit 0
}

Enable-UserLinger -UserName $ServiceUser
Install-UnitFile -UnitName $UnitName -Content $unitContent
& sudo systemctl daemon-reload
if ($LASTEXITCODE -ne 0) {
    throw "systemctl daemon-reload failed"
}
if (-not $NoEnable) {
    & sudo systemctl enable $UnitName
    if ($LASTEXITCODE -ne 0) {
        throw "systemctl enable failed: $UnitName"
    }
}
if (-not $NoStart) {
    & sudo systemctl restart $UnitName
    if ($LASTEXITCODE -ne 0) {
        throw "systemctl restart failed: $UnitName"
    }
}

& systemctl status $UnitName --no-pager
