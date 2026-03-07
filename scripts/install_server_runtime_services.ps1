param(
    [string]$ProjectRoot = "",
    [string]$PythonExe = "",
    [string]$PaperUnitName = "autobot-paper-alpha.service",
    [int]$PaperDurationSec = 0,
    [ValidateSet("live_v3", "live_v4", "candidate_v4", "offline_v4")]
    [string]$PaperPreset = "live_v3",
    [string[]]$PaperCliArgs = @(),
    [switch]$NoStart,
    [switch]$NoEnable
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

. (Join-Path $PSScriptRoot "systemd_service_utils.ps1")

$resolvedProjectRoot = if ([string]::IsNullOrWhiteSpace($ProjectRoot)) { Resolve-DefaultProjectRoot } else { $ProjectRoot }
$resolvedProjectRoot = [System.IO.Path]::GetFullPath($resolvedProjectRoot)
$resolvedPythonExe = if ([string]::IsNullOrWhiteSpace($PythonExe)) { Resolve-DefaultPythonExe -Root $resolvedProjectRoot } else { $PythonExe }
$paperArgList = @(
    "-m", "autobot.cli",
    "paper", "alpha",
    "--duration-sec", [string]([Math]::Max($PaperDurationSec, 0)),
    "--preset", $PaperPreset
) + @($PaperCliArgs)
$paperCommand = ($paperArgList | ForEach-Object { Quote-ShellArg ([string]$_) }) -join " "
$activatePath = Join-Path $resolvedProjectRoot ".venv/bin/activate"
$execStart = "/bin/bash -lc " + (Quote-ShellArg ("source " + $activatePath + " && " + $resolvedPythonExe + " " + $paperCommand))

$paperUnitContent = @"
[Unit]
Description=Autobot Paper Alpha Runtime
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=ubuntu
WorkingDirectory=$resolvedProjectRoot
Environment=PYTHONUNBUFFERED=1
ExecStart=$execStart
Restart=always
RestartSec=15
TimeoutStopSec=30
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
"@

Install-UnitFile -UnitName $PaperUnitName -Content $paperUnitContent
& sudo systemctl daemon-reload
if ($LASTEXITCODE -ne 0) {
    throw "systemctl daemon-reload failed"
}
if (-not $NoEnable) {
    & sudo systemctl enable $PaperUnitName
    if ($LASTEXITCODE -ne 0) {
        throw "systemctl enable failed: $PaperUnitName"
    }
}
if (-not $NoStart) {
    & sudo systemctl restart $PaperUnitName
    if ($LASTEXITCODE -ne 0) {
        throw "systemctl restart failed: $PaperUnitName"
    }
}

& systemctl status $PaperUnitName --no-pager
