param(
    [string]$PythonExe = "C:\Python314\python.exe",
    [string]$ProjectRoot = "D:\MyApps\Autobot",
    [int]$RunHour = 0,
    [int]$RunMinute = 10,
    [int]$TopN = 50,
    [int]$MaxPagesPerTarget = 50,
    [int]$Workers = 1
)

$ErrorActionPreference = "Stop"
Set-Location $ProjectRoot

$mutexName = "Global\AutobotDailyMicroScheduler"
$mutex = [System.Threading.Mutex]::new($false, $mutexName)
$hasMutex = $false
try {
    $hasMutex = $mutex.WaitOne(0)
} catch {
    $hasMutex = $false
}
if (-not $hasMutex) {
    Write-Host "[daily-scheduler] another instance is already running. exiting."
    exit 0
}

$logRoot = Join-Path $ProjectRoot "logs\daily_micro_scheduler"
New-Item -ItemType Directory -Path $logRoot -Force | Out-Null

$stateFile = Join-Path $logRoot "state.json"

function Read-State {
    if (-not (Test-Path $stateFile)) {
        return @{ last_run_date = "" }
    }
    try {
        return Get-Content -Path $stateFile -Raw -Encoding UTF8 | ConvertFrom-Json
    } catch {
        return @{ last_run_date = "" }
    }
}

function Write-State([string]$lastRunDate) {
    $payload = @{
        updated_at = (Get-Date).ToString("o")
        last_run_date = $lastRunDate
    }
    $payload | ConvertTo-Json -Depth 5 | Set-Content -Path $stateFile -Encoding UTF8
}

while ($true) {
    $now = Get-Date
    $today = $now.ToString("yyyy-MM-dd")
    $target = Get-Date -Year $now.Year -Month $now.Month -Day $now.Day -Hour $RunHour -Minute $RunMinute -Second 0
    $state = Read-State
    $lastRunDate = ""
    if ($state -and $state.PSObject.Properties.Name -contains "last_run_date") {
        $lastRunDate = [string]$state.last_run_date
    }

    $shouldRun = ($now -ge $target) -and ($lastRunDate -ne $today)
    if ($shouldRun) {
        $stamp = Get-Date -Format "yyyyMMdd-HHmmss"
        $runDir = Join-Path $logRoot ("run-" + $stamp)
        New-Item -ItemType Directory -Path $runDir -Force | Out-Null
        $stdoutFile = Join-Path $runDir "stdout.log"
        $stderrFile = Join-Path $runDir "stderr.log"

        $args = @(
            "-NoProfile",
            "-ExecutionPolicy", "Bypass",
            "-File", (Join-Path $ProjectRoot "scripts\daily_micro_pipeline.ps1"),
            "-PythonExe", $PythonExe,
            "-ProjectRoot", $ProjectRoot,
            "-TopN", $TopN,
            "-MaxPagesPerTarget", $MaxPagesPerTarget,
            "-Workers", $Workers
        )

        $proc = Start-Process `
            -FilePath "powershell.exe" `
            -ArgumentList $args `
            -WorkingDirectory $ProjectRoot `
            -WindowStyle Hidden `
            -RedirectStandardOutput $stdoutFile `
            -RedirectStandardError $stderrFile `
            -PassThru
        $proc.WaitForExit()

        Write-State -lastRunDate $today
    }

    Start-Sleep -Seconds 30
}

if ($hasMutex) {
    try {
        $mutex.ReleaseMutex() | Out-Null
    } catch {
    }
}
$mutex.Dispose()
