param(
    [string]$PythonExe = "C:\Python314\python.exe",
    [string]$ProjectRoot = "D:\MyApps\Autobot",
    [int]$IntervalSec = 60,
    [string]$LogFile = "logs\ws_public_status_watch.log"
)

$ErrorActionPreference = "Stop"
Set-Location $ProjectRoot

$mutexName = "Global\AutobotWsPublicStatusWatch"
$mutex = [System.Threading.Mutex]::new($false, $mutexName)
$hasMutex = $false
try {
    $hasMutex = $mutex.WaitOne(0)
} catch {
    $hasMutex = $false
}
if (-not $hasMutex) {
    Write-Host "[ws-status-watch] another instance is already running. exiting."
    exit 0
}

$logPath = Join-Path $ProjectRoot $LogFile
New-Item -ItemType Directory -Path (Split-Path -Parent $logPath) -Force | Out-Null

while ($true) {
    $ts = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    "---- $ts ----" | Out-File -Append -Encoding utf8 $logPath
    & $PythonExe -m autobot.cli collect ws-public status --raw-root data/raw_ws/upbit/public --meta-dir data/raw_ws/upbit/_meta |
        Out-File -Append -Encoding utf8 $logPath
    Start-Sleep -Seconds ([Math]::Max($IntervalSec, 5))
}
