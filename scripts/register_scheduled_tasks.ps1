param(
    [string]$PythonExe = "C:\Python314\python.exe",
    [string]$ProjectRoot = "D:\MyApps\Autobot",
    [int]$TopN = 50,
    [int]$RefreshSec = 900,
    [int]$RetentionDays = 30,
    [double]$DownsampleHz = 1.0,
    [int]$MaxMarkets = 60,
    [int]$MaxPagesPerTarget = 50,
    [int]$Workers = 1,
    [int]$DailyHour = 0,
    [int]$DailyMinute = 10
)

$ErrorActionPreference = "Stop"
$schTasksExe = Join-Path $env:SystemRoot "System32\schtasks.exe"
if (-not (Test-Path $schTasksExe)) {
    throw "schtasks.exe not found: $schTasksExe"
}

# Keep /TR short (schtasks limit: 261 chars). Runtime options should live in script defaults/config.
$wsAction = "powershell.exe -NoProfile -WindowStyle Hidden -ExecutionPolicy Bypass -File `"$ProjectRoot\scripts\ws_public_daemon.ps1`""
$dailyAction = "powershell.exe -NoProfile -WindowStyle Hidden -ExecutionPolicy Bypass -File `"$ProjectRoot\scripts\daily_micro_pipeline.ps1`""
$dailyTime = "{0:D2}:{1:D2}" -f [Math]::Max($DailyHour, 0), [Math]::Max($DailyMinute, 0)

function Invoke-SchTasks {
    param(
        [string[]]$TaskArgs,
        [switch]$IgnoreNotFound
    )
    $prevErrorActionPreference = $ErrorActionPreference
    $hasNativePref = $false
    $prevNativePref = $null
    if (Get-Variable -Name PSNativeCommandUseErrorActionPreference -ErrorAction SilentlyContinue) {
        $hasNativePref = $true
        $prevNativePref = $PSNativeCommandUseErrorActionPreference
    }
    try {
        $ErrorActionPreference = "Continue"
        if ($hasNativePref) {
            $PSNativeCommandUseErrorActionPreference = $false
        }
        $output = & $schTasksExe @TaskArgs 2>&1
        $code = $LASTEXITCODE
        if ($output) {
            $output | Out-Host
        }
        if ($code -ne 0) {
            if ($IgnoreNotFound) {
                return $false
            }
            throw "schtasks failed (exit=$code): $schTasksExe $($TaskArgs -join ' ')"
        }
        return $true
    } finally {
        $ErrorActionPreference = $prevErrorActionPreference
        if ($hasNativePref) {
            $PSNativeCommandUseErrorActionPreference = $prevNativePref
        }
    }
}

function Remove-SchTaskIfExists {
    param([string]$TaskName)
    Invoke-SchTasks -TaskArgs @("/Delete", "/TN", $TaskName, "/F") -IgnoreNotFound | Out-Null
}

Remove-SchTaskIfExists -TaskName "Autobot_WS_Public_Daemon"
Remove-SchTaskIfExists -TaskName "Autobot_Daily_Micro_Pipeline"

Invoke-SchTasks -TaskArgs @(
    "/Create", "/TN", "Autobot_WS_Public_Daemon", "/TR", $wsAction,
    "/SC", "ONSTART", "/RU", "SYSTEM", "/RL", "HIGHEST", "/F"
) | Out-Null
Invoke-SchTasks -TaskArgs @(
    "/Create", "/TN", "Autobot_Daily_Micro_Pipeline", "/TR", $dailyAction,
    "/SC", "DAILY", "/ST", $dailyTime, "/RU", "SYSTEM", "/RL", "HIGHEST", "/F"
) | Out-Null

Write-Host ""
Write-Host "[ok] created task: Autobot_WS_Public_Daemon"
Write-Host "[ok] created task: Autobot_Daily_Micro_Pipeline"
Write-Host ""
Write-Host "status check:"
try {
    Invoke-SchTasks -TaskArgs @("/Query", "/TN", "Autobot_WS_Public_Daemon", "/FO", "LIST", "/V") | Out-Null
} catch {
    Write-Host "[warn] cannot query task details for Autobot_WS_Public_Daemon in this shell."
}
try {
    Invoke-SchTasks -TaskArgs @("/Query", "/TN", "Autobot_Daily_Micro_Pipeline", "/FO", "LIST", "/V") | Out-Null
} catch {
    Write-Host "[warn] cannot query task details for Autobot_Daily_Micro_Pipeline in this shell."
}
