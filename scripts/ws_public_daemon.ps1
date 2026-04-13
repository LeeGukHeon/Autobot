param(
    [string]$PythonExe = "C:\Python314\python.exe",
    [string]$ProjectRoot = "D:\MyApps\Autobot",
    [string]$Quote = "KRW",
    [int]$TopN = 30,
    [int]$RefreshSec = 900,
    [int]$RetentionDays = 30,
    [double]$DownsampleHz = 1.0,
    [int]$MaxMarkets = 60,
    [string]$Format = "DEFAULT",
    [int]$DurationSec = 0,
    [int]$MinRestartBackoffSec = 5,
    [int]$MaxRestartBackoffSec = 300
)

$ErrorActionPreference = "Stop"
Set-Location $ProjectRoot

$vendorSitePackages = Join-Path $ProjectRoot "python\site-packages"
if ($IsWindows -and (Test-Path $vendorSitePackages)) {
    if ([string]::IsNullOrWhiteSpace($env:PYTHONPATH)) {
        $env:PYTHONPATH = $vendorSitePackages
    } elseif ($env:PYTHONPATH -notlike "*$vendorSitePackages*") {
        $env:PYTHONPATH = "$vendorSitePackages;$($env:PYTHONPATH)"
    }
}

$mutexName = "Global\AutobotWsPublicDaemon"
$mutex = [System.Threading.Mutex]::new($false, $mutexName)
$hasMutex = $false
try {
    $hasMutex = $mutex.WaitOne(0)
} catch {
    $hasMutex = $false
}
if (-not $hasMutex) {
    Write-Host "[ws-daemon] another instance is already running. exiting."
    exit 0
}

$logRoot = Join-Path $ProjectRoot "logs\ws_public_daemon"
New-Item -ItemType Directory -Path $logRoot -Force | Out-Null

$statusFile = Join-Path $logRoot "status.json"
$restartCount = 0
$backoffSec = [Math]::Max($MinRestartBackoffSec, 1)

while ($true) {
    $stamp = Get-Date -Format "yyyyMMdd-HHmmss"
    $runDir = Join-Path $logRoot ("run-" + $stamp)
    New-Item -ItemType Directory -Path $runDir -Force | Out-Null

    $stdoutFile = Join-Path $runDir "stdout.log"
    $stderrFile = Join-Path $runDir "stderr.log"

    $args = @(
        "-m", "autobot.cli",
        "collect", "ws-public", "daemon",
        "--quote", $Quote,
        "--top-n", $TopN,
        "--refresh-sec", $RefreshSec,
        "--retention-days", $RetentionDays,
        "--downsample-hz", $DownsampleHz,
        "--max-markets", $MaxMarkets,
        "--format", $Format
    )
    if ($DurationSec -gt 0) {
        $args += @("--duration-sec", $DurationSec)
    }

    $startedAt = (Get-Date).ToString("o")
    $proc = Start-Process `
        -FilePath $PythonExe `
        -ArgumentList $args `
        -WorkingDirectory $ProjectRoot `
        -RedirectStandardOutput $stdoutFile `
        -RedirectStandardError $stderrFile `
        -PassThru `
        -NoNewWindow
    $proc.WaitForExit()
    $endedAt = (Get-Date).ToString("o")
    $exitCode = $proc.ExitCode

    $status = [ordered]@{
        updated_at = $endedAt
        last_run = [ordered]@{
            started_at = $startedAt
            ended_at = $endedAt
            exit_code = $exitCode
            stdout = $stdoutFile
            stderr = $stderrFile
            restart_count = $restartCount
            command = "$PythonExe $($args -join ' ')"
        }
    }
    $status | ConvertTo-Json -Depth 8 | Set-Content -Path $statusFile -Encoding UTF8

    if ($DurationSec -gt 0) {
        break
    }

    Start-Sleep -Seconds $backoffSec
    $restartCount += 1
    $backoffSec = [Math]::Min([Math]::Max($backoffSec * 2, 1), [Math]::Max($MaxRestartBackoffSec, 1))
}

if ($hasMutex) {
    try {
        $mutex.ReleaseMutex() | Out-Null
    } catch {
    }
}
$mutex.Dispose()
