param(
    [string]$ProjectRoot = "",
    [string]$PythonExe = "",
    [string]$PaperUnitName = "autobot-paper-alpha.service",
    [int]$PaperDurationSec = 0,
    [string]$PaperPreset = "live_v3",
    [string[]]$PaperCliArgs = @(),
    [switch]$NoStart,
    [switch]$NoEnable
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

function Resolve-DefaultProjectRoot {
    return (Split-Path -Path $PSScriptRoot -Parent)
}

function Resolve-DefaultPythonExe {
    param([string]$Root)
    return (Join-Path $Root ".venv/bin/python")
}

function Resolve-PwshExe {
    $cmd = Get-Command pwsh -ErrorAction SilentlyContinue
    if ($null -ne $cmd -and -not [string]::IsNullOrWhiteSpace($cmd.Source)) {
        return [string]$cmd.Source
    }
    if (Test-Path "/snap/bin/pwsh") {
        return "/snap/bin/pwsh"
    }
    throw "pwsh executable not found"
}

function Quote-ShellArg {
    param([string]$Value)
    if ([string]::IsNullOrWhiteSpace($Value)) {
        return "''"
    }
    return "'" + $Value.Replace("'", "'""'""'") + "'"
}

function Install-UnitFile {
    param(
        [string]$UnitName,
        [string]$Content
    )
    $tmpPath = [System.IO.Path]::GetTempFileName()
    try {
        Set-Content -Path $tmpPath -Encoding UTF8 -Value $Content
        & sudo install -m 0644 $tmpPath ("/etc/systemd/system/" + $UnitName)
        if ($LASTEXITCODE -ne 0) {
            throw "failed to install unit file: $UnitName"
        }
    } finally {
        Remove-Item -Path $tmpPath -Force -ErrorAction SilentlyContinue
    }
}

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
