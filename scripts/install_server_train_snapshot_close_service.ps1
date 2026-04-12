param(
    [string]$ProjectRoot = "",
    [string]$PythonExe = "",
    [string]$ServiceUser = "ubuntu",
    [string]$ServiceUnitName = "autobot-v5-train-snapshot-close.service",
    [string]$TimerUnitName = "autobot-v5-train-snapshot-close.timer",
    [string]$CloseScript = "",
    [string]$Tf = "1m",
    [string]$OnCalendar = "*-*-* 00:05:00",
    [string]$LockFile = "/tmp/autobot-v5-train-snapshot-close.lock",
    [switch]$NoStart,
    [switch]$NoEnable,
    [switch]$DryRun
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

. (Join-Path $PSScriptRoot "systemd_service_utils.ps1")

function Resolve-DefaultCloseScript {
    param([string]$Root)
    return (Join-Path $Root "scripts/close_v5_train_ready_snapshot.ps1")
}

$resolvedProjectRoot = if ([string]::IsNullOrWhiteSpace($ProjectRoot)) { Resolve-DefaultProjectRoot } else { $ProjectRoot }
$resolvedProjectRoot = [System.IO.Path]::GetFullPath($resolvedProjectRoot)
$resolvedPythonExe = if ([string]::IsNullOrWhiteSpace($PythonExe)) { Resolve-DefaultPythonExe -Root $resolvedProjectRoot } else { $PythonExe }
$resolvedCloseScript = if ([string]::IsNullOrWhiteSpace($CloseScript)) { Resolve-DefaultCloseScript -Root $resolvedProjectRoot } else { $CloseScript }
$resolvedPwshExe = Resolve-PwshExe

$closeArgs = @(
    "-NoProfile",
    "-ExecutionPolicy", "Bypass",
    "-File", $resolvedCloseScript,
    "-ProjectRoot", $resolvedProjectRoot,
    "-PythonExe", $resolvedPythonExe,
    "-Tf", ([string]$Tf).Trim().ToLowerInvariant()
)
$closeCommand = $resolvedPwshExe + " " + (($closeArgs | ForEach-Object { Quote-ShellArg ([string]$_) }) -join " ")
$execStart = Build-FlockWrappedExecStart `
    -Command $closeCommand `
    -LockFile $LockFile `
    -BusyMessage "[train-snapshot-close] lock busy, skipping"

$serviceContent = @"
[Unit]
Description=Autobot V5 nightly train snapshot close
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
Description=Autobot V5 nightly train snapshot close timer

[Timer]
OnCalendar=$OnCalendar
Persistent=true
Unit=$ServiceUnitName

[Install]
WantedBy=timers.target
"@

if ($DryRun) {
    Write-Host ("[train-snapshot-close-install][dry-run] service={0}" -f $ServiceUnitName)
    Write-Host ("[train-snapshot-close-install][dry-run] timer={0}" -f $TimerUnitName)
    Write-Host ("[train-snapshot-close-install][dry-run] close_script={0}" -f $resolvedCloseScript)
    Write-Host ("[train-snapshot-close-install][dry-run] lock_file={0}" -f $LockFile)
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
