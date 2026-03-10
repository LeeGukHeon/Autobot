param(
    [string]$ProjectRoot = "",
    [string]$PythonExe = "",
    [string]$ServiceUser = "ubuntu",
    [string]$UnitName = "autobot-dashboard.service",
    [string]$BindHost = "0.0.0.0",
    [int]$Port = 8088,
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
$dashboardCommand = $resolvedPythonExe + " " + ((@(
    "-m", "autobot.dashboard_server",
    "--project-root", $resolvedProjectRoot,
    "--host", $BindHost,
    "--port", ([string]([Math]::Max([int]$Port, 1)))
) | ForEach-Object { Quote-ShellArg ([string]$_) }) -join " ")
$execStart = "/bin/bash -lc " + (Quote-ShellArg $dashboardCommand)

$unitContent = @"
[Unit]
Description=Autobot Operations Dashboard
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=$ServiceUser
WorkingDirectory=$resolvedProjectRoot
Environment=PYTHONUNBUFFERED=1
ExecStart=$execStart
Restart=always
RestartSec=5
TimeoutStopSec=15
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
"@

if ($DryRun) {
    Write-Host ("[dashboard-install][dry-run] unit={0}" -f $UnitName)
    Write-Host ("[dashboard-install][dry-run] host={0}" -f $BindHost)
    Write-Host ("[dashboard-install][dry-run] port={0}" -f $Port)
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
