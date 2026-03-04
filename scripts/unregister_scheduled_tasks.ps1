$ErrorActionPreference = "Continue"

$schTasksExe = Join-Path $env:SystemRoot "System32\schtasks.exe"
if (Test-Path $schTasksExe) {
    function Invoke-SchTasksDelete {
        param([string]$TaskName)
        $stdoutPath = [System.IO.Path]::GetTempFileName()
        $stderrPath = [System.IO.Path]::GetTempFileName()
        try {
            $proc = Start-Process `
                -FilePath $schTasksExe `
                -ArgumentList @("/Delete", "/TN", $TaskName, "/F") `
                -NoNewWindow `
                -Wait `
                -PassThru `
                -RedirectStandardOutput $stdoutPath `
                -RedirectStandardError $stderrPath
            $stdoutText = Get-Content -Path $stdoutPath -Raw -ErrorAction SilentlyContinue
            $stderrText = Get-Content -Path $stderrPath -Raw -ErrorAction SilentlyContinue
            if (-not [string]::IsNullOrWhiteSpace($stdoutText)) {
                $stdoutText.TrimEnd() | Out-Host
            }
            if (-not [string]::IsNullOrWhiteSpace($stderrText)) {
                $stderrText.TrimEnd() | Out-Host
            }
        } finally {
            Remove-Item -Path $stdoutPath -ErrorAction SilentlyContinue
            Remove-Item -Path $stderrPath -ErrorAction SilentlyContinue
        }
    }
    Invoke-SchTasksDelete -TaskName "Autobot_WS_Public_Daemon"
    Invoke-SchTasksDelete -TaskName "Autobot_Daily_Micro_Pipeline"
} else {
    Write-Host "[warn] schtasks.exe not found: $schTasksExe"
}

$runKey = "HKCU:\Software\Microsoft\Windows\CurrentVersion\Run"
Remove-ItemProperty -Path $runKey -Name "Autobot_WS_Public_Daemon" -ErrorAction SilentlyContinue
Remove-ItemProperty -Path $runKey -Name "Autobot_Daily_Micro_Scheduler" -ErrorAction SilentlyContinue

Get-CimInstance Win32_Process |
    Where-Object { $_.CommandLine -match 'ws_public_daemon\.ps1|daily_micro_scheduler\.ps1|daily_micro_pipeline\.ps1|autobot\.cli collect ws-public daemon' } |
    ForEach-Object { Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue }

Write-Host "[ok] cleanup attempted (tasks + run key + processes)."
