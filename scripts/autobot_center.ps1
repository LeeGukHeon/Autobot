param(
    [string]$ProjectRoot = "",
    [string]$RunSelection = "",
    [switch]$UseDefaults,
    [switch]$NoPause,
    [int]$BacktestDaysOverride = 0,
    [int]$PaperDurationSecOverride = 0,
    [string]$TrainerOverride = "",
    [int]$TrainLookbackDaysOverride = 0,
    [int]$ModelBtLookbackDaysOverride = 0
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

function Resolve-ProjectRoot {
    param([string]$Candidate)
    if (-not [string]::IsNullOrWhiteSpace($Candidate)) {
        return [System.IO.Path]::GetFullPath($Candidate)
    }
    return [System.IO.Path]::GetFullPath((Join-Path $PSScriptRoot ".."))
}

$script:ProjectRoot = Resolve-ProjectRoot -Candidate $ProjectRoot
$script:PythonExe = "python"
$script:OpsLogRoot = Join-Path $script:ProjectRoot "logs\ops_center"
$script:ReportsRoot = Join-Path $script:ProjectRoot "docs\reports"
$script:SchedulerTasks = @(
    "Autobot_WS_Public_Daemon",
    "Autobot_Daily_Micro_Pipeline"
)

Set-Location $script:ProjectRoot
New-Item -ItemType Directory -Path $script:OpsLogRoot -Force | Out-Null

function Resolve-PathFromRoot {
    param([string]$PathValue)
    if ([string]::IsNullOrWhiteSpace($PathValue)) {
        return ""
    }
    if ([System.IO.Path]::IsPathRooted($PathValue)) {
        return [System.IO.Path]::GetFullPath($PathValue)
    }
    return [System.IO.Path]::GetFullPath((Join-Path $script:ProjectRoot $PathValue))
}

function New-LogFilePath {
    param([string]$Tag)
    $safeTag = (($Tag -replace "[^a-zA-Z0-9_-]", "_").Trim("_")).ToLowerInvariant()
    if ([string]::IsNullOrWhiteSpace($safeTag)) {
        $safeTag = "operation"
    }
    $stamp = Get-Date -Format "yyyyMMdd-HHmmss"
    return Join-Path $script:OpsLogRoot ("{0}_{1}.log" -f $stamp, $safeTag)
}

function Run-Command {
    param(
        [string]$Description,
        [string]$Exe,
        [string[]]$Arguments,
        [string]$Tag = "operation"
    )

    $logPath = New-LogFilePath -Tag $Tag
    $argText = ($Arguments | ForEach-Object {
            if ($_ -match "\s") {
                '"' + $_ + '"'
            } else {
                $_
            }
        }) -join " "
    $commandLine = "$Exe $argText"
    $lines = New-Object System.Collections.Generic.List[string]

    Add-Content -Path $logPath -Encoding UTF8 -Value ("# description={0}" -f $Description)
    Add-Content -Path $logPath -Encoding UTF8 -Value ("# started_at={0}" -f (Get-Date).ToString("o"))
    Add-Content -Path $logPath -Encoding UTF8 -Value ("# cwd={0}" -f (Get-Location).Path)
    Add-Content -Path $logPath -Encoding UTF8 -Value ("# command={0}" -f $commandLine)

    Write-Host ""
    Write-Host ">>> $Description"
    Write-Host "[cmd] $commandLine"
    Write-Host "[log] $logPath"

    $exitCode = 1
    $previousErrorAction = $ErrorActionPreference
    $hasNativePref = $false
    $previousNativePref = $null
    if (Get-Variable -Name PSNativeCommandUseErrorActionPreference -ErrorAction SilentlyContinue) {
        $hasNativePref = $true
        $previousNativePref = $PSNativeCommandUseErrorActionPreference
    }
    try {
        # Avoid treating native stderr warnings as terminating errors.
        $ErrorActionPreference = "Continue"
        if ($hasNativePref) {
            $PSNativeCommandUseErrorActionPreference = $false
        }
        $output = & $Exe @Arguments 2>&1
        foreach ($entry in @($output)) {
            $text = [string]$entry
            $lines.Add($text) | Out-Null
            Write-Host $text
            Add-Content -Path $logPath -Encoding UTF8 -Value $text
        }
        if ($null -eq $LASTEXITCODE) {
            $exitCode = 0
        } else {
            $exitCode = [int]$LASTEXITCODE
        }
    } catch {
        $errorText = "[error] $($_.Exception.Message)"
        $lines.Add($errorText) | Out-Null
        Write-Host $errorText -ForegroundColor Red
        Add-Content -Path $logPath -Encoding UTF8 -Value $errorText
    } finally {
        $ErrorActionPreference = $previousErrorAction
        if ($hasNativePref) {
            $PSNativeCommandUseErrorActionPreference = $previousNativePref
        }
    }

    Add-Content -Path $logPath -Encoding UTF8 -Value ("# ended_at={0}" -f (Get-Date).ToString("o"))
    Add-Content -Path $logPath -Encoding UTF8 -Value ("# exit_code={0}" -f $exitCode)

    if ($exitCode -eq 0) {
        Write-Host "[exit] 0" -ForegroundColor Green
    } else {
        Write-Host "[exit] $exitCode" -ForegroundColor Yellow
    }

    return [PSCustomObject]@{
        Description = $Description
        Command = $commandLine
        ExitCode = $exitCode
        OutputLines = $lines.ToArray()
        LogFile = $logPath
    }
}

function Confirm-Action {
    param([string]$Prompt)
    $answer = Read-Host "$Prompt (Y/N, default N)"
    return $answer.Trim().ToUpperInvariant() -eq "Y"
}

function Open-Path {
    param([string]$PathValue)
    $resolved = Resolve-PathFromRoot -PathValue $PathValue
    if ([string]::IsNullOrWhiteSpace($resolved)) {
        return
    }
    if (-not (Test-Path $resolved)) {
        Write-Host "[warn] path not found: $resolved" -ForegroundColor Yellow
        return
    }
    Start-Process -FilePath $resolved | Out-Null
}

function Open-Folder {
    param([string]$PathValue)
    Open-Path -PathValue $PathValue
}

function Read-LatestReport {
    param([string]$ReportsPath)

    if (-not (Test-Path $ReportsPath)) {
        return [PSCustomObject]@{
            Found = $false
            Path = ""
            SummaryLines = @()
            LastWriteTime = $null
        }
    }

    $latest = Get-ChildItem -Path $ReportsPath -File -Filter "DAILY_MICRO_REPORT_*.md" |
        Sort-Object LastWriteTimeUtc -Descending |
        Select-Object -First 1

    if ($null -eq $latest) {
        return [PSCustomObject]@{
            Found = $false
            Path = ""
            SummaryLines = @()
            LastWriteTime = $null
        }
    }

    $summaryLines = @()
    $lines = Get-Content -Path $latest.FullName -Encoding UTF8
    $summaryStart = -1
    for ($i = 0; $i -lt $lines.Count; $i += 1) {
        if ($lines[$i] -match "^##\s+Summary") {
            $summaryStart = $i + 1
            break
        }
    }

    if ($summaryStart -ge 0) {
        for ($j = $summaryStart; $j -lt $lines.Count; $j += 1) {
            $line = $lines[$j]
            if ($line -match "^##\s+") {
                break
            }
            if ($line -match "^\s*-\s+") {
                $summaryLines += $line.Trim()
            }
        }
    }

    if ($summaryLines.Count -gt 12) {
        $summaryLines = $summaryLines[0..11]
    }

    return [PSCustomObject]@{
        Found = $true
        Path = $latest.FullName
        SummaryLines = $summaryLines
        LastWriteTime = $latest.LastWriteTime
    }
}

function Get-ObjValue {
    param(
        [Parameter(Mandatory = $false)]$ObjectValue,
        [Parameter(Mandatory = $true)][string]$Name,
        [Parameter(Mandatory = $false)]$DefaultValue = $null
    )
    if ($null -eq $ObjectValue) {
        return $DefaultValue
    }
    if ($ObjectValue -is [System.Collections.IDictionary]) {
        if ($ObjectValue.Contains($Name)) {
            return $ObjectValue[$Name]
        }
        return $DefaultValue
    }
    if ($ObjectValue.PSObject -and $ObjectValue.PSObject.Properties.Name -contains $Name) {
        return $ObjectValue.$Name
    }
    return $DefaultValue
}

function Get-WsHealthSnapshot {
    $healthPath = Resolve-PathFromRoot -PathValue "data/raw_ws/upbit/_meta/ws_public_health.json"
    if (-not (Test-Path $healthPath)) {
        return [PSCustomObject]@{
            Found = $false
            Path = $healthPath
        }
    }

    try {
        $raw = Get-Content -Path $healthPath -Raw -Encoding UTF8
        $doc = $raw | ConvertFrom-Json
    } catch {
        return [PSCustomObject]@{
            Found = $false
            Path = $healthPath
            Error = $_.Exception.Message
        }
    }

    $updatedAtMs = [int64](Get-ObjValue -ObjectValue $doc -Name "updated_at_ms" -DefaultValue 0)
    $lastRxTs = Get-ObjValue -ObjectValue $doc -Name "last_rx_ts_ms" -DefaultValue $null
    $nowMs = [DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds()

    $healthLagSec = $null
    if ($updatedAtMs -gt 0) {
        $healthLagSec = [Math]::Round(($nowMs - $updatedAtMs) / 1000.0, 3)
    }

    $tradeLagSec = $null
    $orderbookLagSec = $null
    if ($null -ne $lastRxTs) {
        $tradeTs = [int64](Get-ObjValue -ObjectValue $lastRxTs -Name "trade" -DefaultValue 0)
        $orderbookTs = [int64](Get-ObjValue -ObjectValue $lastRxTs -Name "orderbook" -DefaultValue 0)
        if ($tradeTs -gt 0) {
            $tradeLagSec = [Math]::Round(($nowMs - $tradeTs) / 1000.0, 3)
        }
        if ($orderbookTs -gt 0) {
            $orderbookLagSec = [Math]::Round(($nowMs - $orderbookTs) / 1000.0, 3)
        }
    }

    return [PSCustomObject]@{
        Found = $true
        Path = $healthPath
        Connected = [bool](Get-ObjValue -ObjectValue $doc -Name "connected" -DefaultValue $false)
        SubscribedMarketsCount = [int](Get-ObjValue -ObjectValue $doc -Name "subscribed_markets_count" -DefaultValue 0)
        HealthLagSec = $healthLagSec
        TradeLagSec = $tradeLagSec
        OrderbookLagSec = $orderbookLagSec
        RunId = [string](Get-ObjValue -ObjectValue $doc -Name "run_id" -DefaultValue "")
    }
}

function Get-SchedulerTaskStatus {
    param([string]$TaskName)

    function Parse-SchtasksField {
        param(
            [string[]]$Lines,
            [string[]]$Keys
        )
        foreach ($line in $Lines) {
            foreach ($key in $Keys) {
                $pattern = "^\s*" + [Regex]::Escape($key) + "\s*:\s*(.+)$"
                if ($line -match $pattern) {
                    return $matches[1].Trim()
                }
            }
        }
        return ""
    }

    function Query-SchtasksExact {
        param([string]$Name)
        $candidates = @()
        if (-not [string]::IsNullOrWhiteSpace($Name)) {
            if ($Name.StartsWith("\")) {
                $candidates += $Name
            } else {
                $candidates += ("\" + $Name)
                $candidates += $Name
            }
        }

        $lastResult = [PSCustomObject]@{
            ExitCode = 1
            OutputLines = @()
            OutputText = ""
        }
        foreach ($candidate in $candidates) {
            $previousErrorAction = $ErrorActionPreference
            $hasNativePref = $false
            $previousNativePref = $null
            if (Get-Variable -Name PSNativeCommandUseErrorActionPreference -ErrorAction SilentlyContinue) {
                $hasNativePref = $true
                $previousNativePref = $PSNativeCommandUseErrorActionPreference
            }
            try {
                $ErrorActionPreference = "Continue"
                if ($hasNativePref) {
                    $PSNativeCommandUseErrorActionPreference = $false
                }
                $output = & schtasks.exe /Query /TN $candidate /FO LIST /V 2>&1
                $lines = @($output | ForEach-Object { [string]$_ })
                $result = [PSCustomObject]@{
                    ExitCode = [int]$LASTEXITCODE
                    OutputLines = $lines
                    OutputText = ($lines -join "`n")
                }
                if ($result.ExitCode -eq 0) {
                    return $result
                }
                $lastResult = $result
            } finally {
                $ErrorActionPreference = $previousErrorAction
                if ($hasNativePref) {
                    $PSNativeCommandUseErrorActionPreference = $previousNativePref
                }
            }
        }
        return $lastResult
    }

    try {
        $task = Get-ScheduledTask -TaskName $TaskName -ErrorAction Stop
        $info = Get-ScheduledTaskInfo -TaskName $TaskName -ErrorAction Stop
        return [PSCustomObject]@{
            TaskName = $TaskName
            Found = $true
            AccessDenied = $false
            State = [string]$task.State
            LastRunTime = $info.LastRunTime
            NextRunTime = $info.NextRunTime
            LastTaskResult = $info.LastTaskResult
            Error = ""
        }
    } catch {
        try {
            $allTasks = @(Get-ScheduledTask -ErrorAction Stop)
            $target = (($TaskName.Trim().ToLowerInvariant()) -replace "[\\/_\-\s]+", "")
            $resolved = $allTasks | Where-Object {
                $nameNorm = (([string]$_.TaskName).Trim().ToLowerInvariant() -replace "[\\/_\-\s]+", "")
                $nameNorm -eq $target
            } | Select-Object -First 1
            if ($null -ne $resolved) {
                $resolvedPath = if ([string]::IsNullOrWhiteSpace([string]$resolved.TaskPath)) { "\" } else { [string]$resolved.TaskPath }
                $resolvedInfo = Get-ScheduledTaskInfo -TaskName $resolved.TaskName -TaskPath $resolvedPath -ErrorAction Stop
                return [PSCustomObject]@{
                    TaskName = $TaskName
                    Found = $true
                    AccessDenied = $false
                    State = [string]$resolved.State
                    LastRunTime = $resolvedInfo.LastRunTime
                    NextRunTime = $resolvedInfo.NextRunTime
                    LastTaskResult = $resolvedInfo.LastTaskResult
                    Error = ""
                }
            }
        } catch {
            # Continue to schtasks fallback.
        }

        $schtasksResult = Query-SchtasksExact -Name $TaskName
        if ($schtasksResult.ExitCode -eq 0) {
            $stateValue = Parse-SchtasksField -Lines $schtasksResult.OutputLines -Keys @("Status", "상태")
            $lastRunText = Parse-SchtasksField -Lines $schtasksResult.OutputLines -Keys @("Last Run Time", "마지막 실행 시간")
            $nextRunText = Parse-SchtasksField -Lines $schtasksResult.OutputLines -Keys @("Next Run Time", "다음 실행 시간")
            $lastResultText = Parse-SchtasksField -Lines $schtasksResult.OutputLines -Keys @("Last Result", "마지막 결과")
            return [PSCustomObject]@{
                TaskName = $TaskName
                Found = $true
                AccessDenied = $false
                State = if ([string]::IsNullOrWhiteSpace($stateValue)) { "UNKNOWN" } else { $stateValue }
                LastRunTime = if ([string]::IsNullOrWhiteSpace($lastRunText)) { $null } else { $lastRunText }
                NextRunTime = if ([string]::IsNullOrWhiteSpace($nextRunText)) { $null } else { $nextRunText }
                LastTaskResult = if ([string]::IsNullOrWhiteSpace($lastResultText)) { $null } else { $lastResultText }
                Error = ""
            }
        }

        $outputLower = $schtasksResult.OutputText.ToLowerInvariant()
        if ($outputLower.Contains("access is denied") -or $outputLower.Contains("액세스가 거부")) {
            return [PSCustomObject]@{
                TaskName = $TaskName
                Found = $true
                AccessDenied = $true
                State = "ACCESS_DENIED"
                LastRunTime = $null
                NextRunTime = $null
                LastTaskResult = $null
                Error = "Task exists but current shell has insufficient permission."
            }
        }

        return [PSCustomObject]@{
            TaskName = $TaskName
            Found = $false
            AccessDenied = $false
            State = "N/A"
            LastRunTime = $null
            NextRunTime = $null
            LastTaskResult = $null
            Error = if (-not [string]::IsNullOrWhiteSpace($schtasksResult.OutputText)) { $schtasksResult.OutputText } else { $_.Exception.Message }
        }
    }
}

function Format-DateTimeSafe {
    param($Value)
    if ($null -eq $Value) {
        return "N/A"
    }
    try {
        $dt = [DateTime]$Value
        if ($dt.Year -le 1900) {
            return "N/A"
        }
        return $dt.ToString("yyyy-MM-dd HH:mm:ss")
    } catch {
        return "N/A"
    }
}

function Invoke-Preflight {
    param([string]$Tag)

    Set-Location $script:ProjectRoot
    Write-Host "[root] $script:ProjectRoot"
    $check = Run-Command `
        -Description "CLI preflight (python -m autobot.cli --help)" `
        -Exe $script:PythonExe `
        -Arguments @("-m", "autobot.cli", "--help") `
        -Tag ("preflight_" + $Tag)

    if ($check.ExitCode -ne 0) {
        Write-Host "[error] preflight failed. check log: $($check.LogFile)" -ForegroundColor Red
        return $false
    }
    return $true
}

function Set-FeaturesV3OneMSynthWeightPower {
    param([double]$Power = 2.0)

    $configPath = Resolve-PathFromRoot -PathValue "config/features_v3.yaml"
    if (-not (Test-Path $configPath)) {
        Write-Host "[warn] features_v3 config not found: $configPath" -ForegroundColor Yellow
        return $false
    }

    $raw = Get-Content -Path $configPath -Raw -Encoding UTF8
    $formattedPower = ("{0:0.0}" -f $Power)
    $pattern = "(?m)^(\s*one_m_synth_weight_power:\s*)([0-9]+(?:\.[0-9]+)?)\s*$"
    if (-not [regex]::IsMatch($raw, $pattern)) {
        Write-Host "[warn] one_m_synth_weight_power key not found in $configPath" -ForegroundColor Yellow
        return $false
    }

    $updated = [regex]::Replace($raw, $pattern, ("`$1{0}" -f $formattedPower), 1)
    if ($updated -ne $raw) {
        Set-Content -Path $configPath -Value $updated -Encoding UTF8
        Write-Host "[config] features_v3.one_m_synth_weight_power -> $formattedPower"
    } else {
        Write-Host "[config] features_v3.one_m_synth_weight_power already $formattedPower"
    }
    return $true
}

function Read-DefaultValue {
    param(
        [string]$Prompt,
        [string]$DefaultValue
    )
    $raw = Read-Host "$Prompt [$DefaultValue]"
    if ([string]::IsNullOrWhiteSpace($raw)) {
        return $DefaultValue
    }
    return $raw.Trim()
}

function Read-DefaultInt {
    param(
        [string]$Prompt,
        [int]$DefaultValue
    )
    while ($true) {
        $text = Read-Host "$Prompt [$DefaultValue]"
        if ([string]::IsNullOrWhiteSpace($text)) {
            return $DefaultValue
        }
        $parsed = 0
        if ([int]::TryParse($text.Trim(), [ref]$parsed)) {
            return $parsed
        }
        Write-Host "[warn] enter integer value." -ForegroundColor Yellow
    }
}

function Get-LatestChildDirectory {
    param([string]$RelativePath)
    $root = Resolve-PathFromRoot -PathValue $RelativePath
    if (-not (Test-Path $root)) {
        return $null
    }
    $latest = Get-ChildItem -Path $root -Directory |
        Sort-Object LastWriteTimeUtc -Descending |
        Select-Object -First 1
    if ($null -eq $latest) {
        return $null
    }
    return $latest.FullName
}

function Get-LatestModelBtRunDirectory {
    $root = Resolve-PathFromRoot -PathValue "data/backtest/runs"
    if (-not (Test-Path $root)) {
        return $null
    }
    $latest = Get-ChildItem -Path $root -Directory -Filter "modelbt-*" |
        Sort-Object LastWriteTimeUtc -Descending |
        Select-Object -First 1
    if ($null -eq $latest) {
        return $null
    }
    return $latest.FullName
}

function Get-RunDirFromOutput {
    param([string[]]$OutputLines)
    foreach ($line in $OutputLines) {
        if ($line -match '"run_dir"\s*:\s*"([^"]+)"') {
            return $matches[1]
        }
        if ($line -match 'run_dir=(.+)$') {
            return $matches[1].Trim()
        }
    }
    return $null
}

function Get-ReportHints {
    param([string[]]$OutputLines)
    $hints = New-Object System.Collections.Generic.List[string]
    foreach ($line in $OutputLines) {
        if ($line -match 'report=([^\s]+)') {
            $hints.Add($matches[1]) | Out-Null
        }
        if ($line -match '"report"\s*:\s*"([^"]+)"') {
            $hints.Add($matches[1]) | Out-Null
        }
    }
    return @($hints | Select-Object -Unique)
}

function Wait-ReturnToMenu {
    [void](Read-Host "`nPress Enter to return to menu")
}

function Show-StatusDashboard {
    Write-Host ""
    Write-Host "== Status Dashboard =="

    $latestReport = Read-LatestReport -ReportsPath $script:ReportsRoot
    if ($latestReport.Found) {
        Write-Host "[daily] latest_report=$($latestReport.Path)"
        Write-Host "[daily] updated_at=$($latestReport.LastWriteTime.ToString("yyyy-MM-dd HH:mm:ss"))"
        if ($latestReport.SummaryLines.Count -gt 0) {
            Write-Host "[daily] summary:"
            foreach ($line in $latestReport.SummaryLines) {
                Write-Host ("  {0}" -f $line)
            }
        }
    } else {
        Write-Host "[daily] no DAILY_MICRO_REPORT_*.md found."
    }

    $ws = Get-WsHealthSnapshot
    if ($ws.Found) {
        Write-Host ("[ws] connected={0} lag_sec={1} subscribed={2} trade_lag_sec={3} orderbook_lag_sec={4}" -f `
                $ws.Connected, $ws.HealthLagSec, $ws.SubscribedMarketsCount, $ws.TradeLagSec, $ws.OrderbookLagSec)
        Write-Host "[ws] run_id=$($ws.RunId)"
        Write-Host "[ws] health_file=$($ws.Path)"
    } else {
        if ($ws.PSObject.Properties.Name -contains "Error") {
            Write-Host "[ws] health snapshot parse error: $($ws.Error)" -ForegroundColor Yellow
        } else {
            Write-Host "[ws] health snapshot not found: $($ws.Path)" -ForegroundColor Yellow
        }
    }

    Write-Host "[scheduler]"
    foreach ($taskName in $script:SchedulerTasks) {
        $status = Get-SchedulerTaskStatus -TaskName $taskName
        if ($status.Found) {
            if ($status.AccessDenied) {
                Write-Host ("  - {0}: state=ACCESS_DENIED (run Control Center as Administrator to view details)" -f $taskName) -ForegroundColor Yellow
            } else {
                Write-Host ("  - {0}: state={1}, last={2}, next={3}, result={4}" -f `
                        $taskName, `
                        $status.State, `
                        (Format-DateTimeSafe -Value $status.LastRunTime), `
                        (Format-DateTimeSafe -Value $status.NextRunTime), `
                        $status.LastTaskResult)
            }
        } else {
            Write-Host ("  - {0}: not found ({1})" -f $taskName, $status.Error) -ForegroundColor Yellow
        }
    }
}

function Invoke-DailyPipelineNow {
    $scriptPath = Resolve-PathFromRoot -PathValue "scripts/daily_micro_pipeline.ps1"
    $result = Run-Command `
        -Description "Run Daily Pipeline Now" `
        -Exe "powershell.exe" `
        -Arguments @("-NoProfile", "-ExecutionPolicy", "Bypass", "-File", $scriptPath) `
        -Tag "menu2_daily_pipeline"

    if ($result.ExitCode -ne 0) {
        Write-Host "[daily] failed. log: $($result.LogFile)" -ForegroundColor Yellow
        return
    }

    $latest = Read-LatestReport -ReportsPath $script:ReportsRoot
    if ($latest.Found) {
        Write-Host "[daily] latest_report=$($latest.Path)"
        Open-Path -PathValue $latest.Path
    }
}

function Invoke-WsDaemonTaskControl {
    $taskName = "Autobot_WS_Public_Daemon"
    $taskNameForSchtasks = "\" + $taskName
    Write-Host ""
    Write-Host "1) Start task (Autobot_WS_Public_Daemon)"
    Write-Host "2) Stop task (Autobot_WS_Public_Daemon)"
    Write-Host "3) Back"
    $choice = Read-Host "Select action"
    switch ($choice) {
        "1" {
            $result = Run-Command `
                -Description "Start scheduled task Autobot_WS_Public_Daemon" `
                -Exe "schtasks.exe" `
                -Arguments @("/Run", "/TN", $taskNameForSchtasks) `
                -Tag "menu3_start_ws_daemon_task"
            if ($result.ExitCode -ne 0) {
                Write-Host "[task] start failed. log: $($result.LogFile)" -ForegroundColor Yellow
            }
        }
        "2" {
            if (-not (Confirm-Action -Prompt "Stop task Autobot_WS_Public_Daemon now?")) {
                Write-Host "[task] stop cancelled."
                return
            }
            $result = Run-Command `
                -Description "Stop scheduled task Autobot_WS_Public_Daemon" `
                -Exe "schtasks.exe" `
                -Arguments @("/End", "/TN", $taskNameForSchtasks) `
                -Tag "menu3_stop_ws_daemon_task"
            if ($result.ExitCode -ne 0) {
                Write-Host "[task] stop failed. log: $($result.LogFile)" -ForegroundColor Yellow
            }
        }
        default {
            return
        }
    }
    $status = Get-SchedulerTaskStatus -TaskName $taskName
    if ($status.Found) {
        Write-Host ("[task] state={0}, last={1}, next={2}, result={3}" -f `
                $status.State, `
                (Format-DateTimeSafe -Value $status.LastRunTime), `
                (Format-DateTimeSafe -Value $status.NextRunTime), `
                $status.LastTaskResult)
    }
}

function Invoke-BacktestWizard {
    param(
        [switch]$UseDefaultsMode,
        [int]$DaysOverride = 0
    )

    $tf = "5m"
    $quote = "KRW"
    $topN = 20
    $days = 8

    if (-not $UseDefaultsMode) {
        $tf = Read-DefaultValue -Prompt "Timeframe" -DefaultValue $tf
        $quote = Read-DefaultValue -Prompt "Quote" -DefaultValue $quote
        $topN = Read-DefaultInt -Prompt "Top N universe" -DefaultValue $topN
        $days = Read-DefaultInt -Prompt "Duration days" -DefaultValue $days
    }
    if ($DaysOverride -gt 0) {
        $days = $DaysOverride
    }

    $beforeLatest = Get-LatestChildDirectory -RelativePath "data/backtest/runs"
    $args = @(
        "-m", "autobot.cli",
        "backtest", "run",
        "--tf", $tf,
        "--quote", $quote,
        "--top-n", "$topN",
        "--duration-days", "$days"
    )
    $result = Run-Command `
        -Description "Backtest Wizard Run" `
        -Exe $script:PythonExe `
        -Arguments $args `
        -Tag "menu4_backtest_wizard"

    if ($result.ExitCode -ne 0) {
        Write-Host "[backtest] failed. log: $($result.LogFile)" -ForegroundColor Yellow
        return
    }

    $runDir = Get-RunDirFromOutput -OutputLines $result.OutputLines
    if ([string]::IsNullOrWhiteSpace($runDir)) {
        $afterLatest = Get-LatestChildDirectory -RelativePath "data/backtest/runs"
        if ($null -ne $afterLatest) {
            $runDir = $afterLatest
        } else {
            $runDir = $beforeLatest
        }
    }
    if (-not [string]::IsNullOrWhiteSpace($runDir)) {
        Write-Host "[backtest] run_dir=$runDir"
        Open-Folder -PathValue $runDir
    }
}

function Invoke-PaperWizard {
    param(
        [switch]$UseDefaultsMode,
        [int]$DurationSecOverride = 0
    )

    $durationSec = 600
    $quote = "KRW"
    $topN = 20

    if (-not $UseDefaultsMode) {
        $durationSec = Read-DefaultInt -Prompt "Duration sec (recommended 600 or 3600)" -DefaultValue $durationSec
        $quote = Read-DefaultValue -Prompt "Quote" -DefaultValue $quote
        $topN = Read-DefaultInt -Prompt "Top N universe" -DefaultValue $topN
    }
    if ($DurationSecOverride -gt 0) {
        $durationSec = $DurationSecOverride
    }

    $beforeLatest = Get-LatestChildDirectory -RelativePath "data/paper/runs"
    $args = @(
        "-m", "autobot.cli",
        "paper", "run",
        "--duration-sec", "$durationSec",
        "--quote", $quote,
        "--top-n", "$topN"
    )
    $result = Run-Command `
        -Description "Paper Wizard Run" `
        -Exe $script:PythonExe `
        -Arguments $args `
        -Tag "menu5_paper_wizard"

    if ($result.ExitCode -ne 0) {
        Write-Host "[paper] failed. log: $($result.LogFile)" -ForegroundColor Yellow
        return
    }

    $runDir = Get-RunDirFromOutput -OutputLines $result.OutputLines
    if ([string]::IsNullOrWhiteSpace($runDir)) {
        $afterLatest = Get-LatestChildDirectory -RelativePath "data/paper/runs"
        if ($null -ne $afterLatest) {
            $runDir = $afterLatest
        } else {
            $runDir = $beforeLatest
        }
    }
    if (-not [string]::IsNullOrWhiteSpace($runDir)) {
        Write-Host "[paper] run_dir=$runDir"
        Open-Folder -PathValue $runDir
    }
}

function Invoke-ModelTrainWizard {
    param(
        [switch]$UseDefaultsMode,
        [string]$TrainerOverrideValue = "",
        [int]$LookbackDaysOverride = 0
    )

    $trainer = "v3_mtf_micro"
    $lookbackDays = 30
    $tf = "5m"
    $quote = "KRW"
    $topN = 50
    $labelSet = "v1"
    $task = "cls"

    if (-not $UseDefaultsMode) {
        $trainer = (Read-DefaultValue -Prompt "Trainer (v1, v2_micro, v3_mtf_micro, v4_crypto_cs, v5_panel_ensemble)" -DefaultValue $trainer).ToLowerInvariant()
        $lookbackDays = Read-DefaultInt -Prompt "Train window lookback days" -DefaultValue $lookbackDays
        $tf = Read-DefaultValue -Prompt "Timeframe" -DefaultValue $tf
        $quote = Read-DefaultValue -Prompt "Quote" -DefaultValue $quote
        $topN = Read-DefaultInt -Prompt "Top N universe" -DefaultValue $topN
    }

    if (-not [string]::IsNullOrWhiteSpace($TrainerOverrideValue)) {
        $trainer = $TrainerOverrideValue.ToLowerInvariant()
    }
    if ($LookbackDaysOverride -gt 0) {
        $lookbackDays = $LookbackDaysOverride
    }

    if ($trainer -eq "v2") { $trainer = "v2_micro" }
    if ($trainer -eq "v3") { $trainer = "v3_mtf_micro" }
    if ($trainer -eq "v4") { $trainer = "v4_crypto_cs" }
    if ($trainer -eq "v5") { $trainer = "v5_panel_ensemble" }

    if (@("v1", "v2_micro", "v3_mtf_micro", "v4_crypto_cs", "v5_panel_ensemble") -notcontains $trainer) {
        Write-Host "[warn] invalid trainer. fallback to v3_mtf_micro." -ForegroundColor Yellow
        $trainer = "v3_mtf_micro"
    }

    if ($lookbackDays -lt 8) {
        $lookbackDays = 8
    }

    $endDate = (Get-Date).Date.AddDays(-1).ToString("yyyy-MM-dd")
    $startDate = (Get-Date).Date.AddDays(-1 * $lookbackDays).ToString("yyyy-MM-dd")
    $featureSet = "v1"
    $family = "train_v1"
    switch ($trainer) {
        "v2_micro" {
            $featureSet = "v2"
            $family = "train_v2_micro"
        }
        "v3_mtf_micro" {
            $featureSet = "v3"
            $family = "train_v3_mtf_micro"
        }
        "v4_crypto_cs" {
            $featureSet = "v4"
            $family = "train_v4_crypto_cs"
            $labelSet = "v3"
            $task = "cls"
        }
        "v5_panel_ensemble" {
            $featureSet = "v4"
            $family = "train_v5_panel_ensemble"
            $labelSet = "v3"
            $task = "cls"
        }
        default {
            $featureSet = "v1"
            $family = "train_v1"
        }
    }

    if ($trainer -eq "v3_mtf_micro") {
        $enforced = Set-FeaturesV3OneMSynthWeightPower -Power 2.0
        if (-not $enforced) {
            Write-Host "[model] cannot enforce features_v3.one_m_synth_weight_power=2.0. aborting train wizard." -ForegroundColor Yellow
            return
        }

        $buildArgs = @(
            "-m", "autobot.cli",
            "features", "build",
            "--feature-set", "v3",
            "--tf", $tf,
            "--quote", $quote,
            "--top-n", "$topN",
            "--start", $startDate,
            "--end", $endDate
        )
        $buildResult = Run-Command `
            -Description "Features Build Wizard (v3, one_m_synth_weight_power=2.0)" `
            -Exe $script:PythonExe `
            -Arguments $buildArgs `
            -Tag "menu6_features_build_v3"

        if ($buildResult.ExitCode -ne 0) {
            Write-Host "[model] features build failed. log: $($buildResult.LogFile)" -ForegroundColor Yellow
            return
        }
    }

    if (@("v4_crypto_cs", "v5_panel_ensemble") -contains $trainer) {
        $buildArgs = @(
            "-m", "autobot.cli",
            "features", "build",
            "--feature-set", "v4",
            "--label-set", $labelSet,
            "--tf", $tf,
            "--quote", $quote,
            "--top-n", "$topN",
            "--start", $startDate,
            "--end", $endDate
        )
        $buildResult = Run-Command `
            -Description ("Features Build Wizard (v4, label_" + $labelSet + ", trainer_" + $trainer + ")") `
            -Exe $script:PythonExe `
            -Arguments $buildArgs `
            -Tag "menu6_features_build_v4"

        if ($buildResult.ExitCode -ne 0) {
            Write-Host "[model] features build failed. log: $($buildResult.LogFile)" -ForegroundColor Yellow
            return
        }
    }

    $trainArgs = @(
        "-m", "autobot.cli",
        "model", "train",
        "--trainer", $trainer,
        "--feature-set", $featureSet,
        "--label-set", $labelSet,
        "--model-family", $family,
        "--tf", $tf,
        "--quote", $quote,
        "--top-n", "$topN",
        "--start", $startDate,
        "--end", $endDate
    )
    if ($trainer -eq "v4_crypto_cs") {
        $trainArgs += @("--task", $task)
    }
    $trainResult = Run-Command `
        -Description "Model Train Wizard ($trainer)" `
        -Exe $script:PythonExe `
        -Arguments $trainArgs `
        -Tag "menu6_model_train"

    if ($trainResult.ExitCode -ne 0) {
        Write-Host "[model] train failed. log: $($trainResult.LogFile)" -ForegroundColor Yellow
        return
    }

    $evalArgs = @(
        "-m", "autobot.cli",
        "model", "eval",
        "--model-ref", "champion",
        "--model-family", $family,
        "--split", "test"
    )
    $evalResult = Run-Command `
        -Description "Model Eval Wizard ($family champion test)" `
        -Exe $script:PythonExe `
        -Arguments $evalArgs `
        -Tag "menu6_model_eval"

    if ($evalResult.ExitCode -ne 0) {
        Write-Host "[model] eval failed. log: $($evalResult.LogFile)" -ForegroundColor Yellow
    }

    $runDir = Get-RunDirFromOutput -OutputLines $trainResult.OutputLines
    $registryFamilyDir = Resolve-PathFromRoot -PathValue ("models/registry/" + $family)
    Write-Host "[model] registry=$registryFamilyDir"
    Open-Folder -PathValue $registryFamilyDir

    if (-not [string]::IsNullOrWhiteSpace($runDir)) {
        $metricsPath = Join-Path (Resolve-PathFromRoot -PathValue $runDir) "metrics.json"
        if (Test-Path $metricsPath) {
            Write-Host "[model] metrics=$metricsPath"
            Open-Path -PathValue $metricsPath
        } else {
            Write-Host "[model] metrics not found in run dir: $metricsPath" -ForegroundColor Yellow
        }
    }
}

function Invoke-ValidateDoctor {
    param([switch]$UseDefaultsMode)
    $quarantineCorrupt = "false"
    if (-not $UseDefaultsMode) {
        if (Confirm-Action -Prompt "Enable quarantine move for corrupt ws files?") {
            if (Confirm-Action -Prompt "This may move files into quarantine. Continue?") {
                $quarantineCorrupt = "true"
            }
        }
    }

    $checks = @(
        [PSCustomObject]@{
            Name = "ws_public_validate"
            Description = "Doctor: ws-public validate"
            Exe = $script:PythonExe
            Args = @(
                "-m", "autobot.cli",
                "collect", "ws-public", "validate",
                "--raw-root", "data/raw_ws/upbit/public",
                "--meta-dir", "data/raw_ws/upbit/_meta",
                "--quarantine-corrupt", $quarantineCorrupt,
                "--min-age-sec", "300"
            )
        },
        [PSCustomObject]@{
            Name = "ws_public_stats"
            Description = "Doctor: ws-public stats"
            Exe = $script:PythonExe
            Args = @(
                "-m", "autobot.cli",
                "collect", "ws-public", "stats",
                "--raw-root", "data/raw_ws/upbit/public",
                "--meta-dir", "data/raw_ws/upbit/_meta"
            )
        },
        [PSCustomObject]@{
            Name = "micro_validate"
            Description = "Doctor: micro validate"
            Exe = $script:PythonExe
            Args = @(
                "-m", "autobot.cli",
                "micro", "validate",
                "--out-root", "data/parquet/micro_v1"
            )
        },
        [PSCustomObject]@{
            Name = "micro_stats"
            Description = "Doctor: micro stats"
            Exe = $script:PythonExe
            Args = @(
                "-m", "autobot.cli",
                "micro", "stats",
                "--out-root", "data/parquet/micro_v1"
            )
        },
        [PSCustomObject]@{
            Name = "candles_validate"
            Description = "Doctor: candles validate"
            Exe = $script:PythonExe
            Args = @(
                "-m", "autobot.cli",
                "data", "validate",
                "--parquet-dir", "data/parquet/candles_api_v1"
            )
        }
    )

    $failureCount = 0
    foreach ($check in $checks) {
        $result = Run-Command `
            -Description $check.Description `
            -Exe $check.Exe `
            -Arguments $check.Args `
            -Tag ("menu7_" + $check.Name)
        if ($result.ExitCode -ne 0) {
            $failureCount += 1
            $hints = Get-ReportHints -OutputLines $result.OutputLines
            if ($hints.Count -gt 0) {
                Write-Host "[doctor] inspect reports/files:" -ForegroundColor Yellow
                foreach ($hint in $hints) {
                    Write-Host ("  - {0}" -f (Resolve-PathFromRoot -PathValue $hint)) -ForegroundColor Yellow
                }
            }
            Write-Host "[doctor] failed command log: $($result.LogFile)" -ForegroundColor Yellow
        }
    }

    if ($failureCount -eq 0) {
        Write-Host "[doctor] all checks completed successfully." -ForegroundColor Green
    } else {
        Write-Host "[doctor] failures=$failureCount (see logs under $script:OpsLogRoot)." -ForegroundColor Yellow
    }
}

function Invoke-ModelBtWizard {
    param(
        [switch]$UseDefaultsMode,
        [int]$LookbackDaysOverride = 0
    )

    $modelRef = "champion_v3"
    $modelFamily = "train_v3_mtf_micro"
    $tf = "5m"
    $quote = "KRW"
    $topN = 50
    $lookbackDays = 8
    $topPct = 0.05
    $holdBars = 6
    $feeBps = 5.0

    if (-not $UseDefaultsMode) {
        $modelRef = Read-DefaultValue -Prompt "Model ref (champion_v3/champion_v4/latest_candidate_v3/latest_candidate_v4/latest/run_id)" -DefaultValue $modelRef
        $modelFamily = Read-DefaultValue -Prompt "Model family" -DefaultValue $modelFamily
        $lookbackDays = Read-DefaultInt -Prompt "ModelBT lookback days" -DefaultValue $lookbackDays
        $tf = Read-DefaultValue -Prompt "Timeframe" -DefaultValue $tf
        $quote = Read-DefaultValue -Prompt "Quote" -DefaultValue $quote
        $topN = Read-DefaultInt -Prompt "Top N universe" -DefaultValue $topN
        $topPct = [double](Read-DefaultValue -Prompt "Top pct (0~1)" -DefaultValue "$topPct")
        $holdBars = Read-DefaultInt -Prompt "Hold bars" -DefaultValue $holdBars
        $feeBps = [double](Read-DefaultValue -Prompt "Fee bps" -DefaultValue "$feeBps")
    }

    if ($LookbackDaysOverride -gt 0) {
        $lookbackDays = $LookbackDaysOverride
    }
    if ($lookbackDays -lt 2) {
        $lookbackDays = 2
    }
    if ($topPct -le 0.0) {
        $topPct = 0.05
    }
    if ($topPct -gt 1.0) {
        $topPct = 1.0
    }
    if ($holdBars -lt 1) {
        $holdBars = 1
    }

    $endDate = (Get-Date).Date.AddDays(-1).ToString("yyyy-MM-dd")
    $startDate = (Get-Date).Date.AddDays(-1 * $lookbackDays).ToString("yyyy-MM-dd")
    $beforeLatest = Get-LatestModelBtRunDirectory

    $args = @(
        "-m", "autobot.cli",
        "modelbt", "run",
        "--model-ref", $modelRef,
        "--model-family", $modelFamily,
        "--tf", $tf,
        "--quote", $quote,
        "--top-n", "$topN",
        "--start", $startDate,
        "--end", $endDate,
        "--select", "top_pct",
        "--top-pct", "$topPct",
        "--hold-bars", "$holdBars",
        "--fee-bps", "$feeBps"
    )
    $result = Run-Command `
        -Description "ModelBT Proxy Wizard ($modelRef / $modelFamily)" `
        -Exe $script:PythonExe `
        -Arguments $args `
        -Tag "menu9_modelbt_wizard"

    if ($result.ExitCode -ne 0) {
        Write-Host "[modelbt] failed. log: $($result.LogFile)" -ForegroundColor Yellow
        return
    }

    $runDir = Get-RunDirFromOutput -OutputLines $result.OutputLines
    if ([string]::IsNullOrWhiteSpace($runDir)) {
        $afterLatest = Get-LatestModelBtRunDirectory
        if ($null -ne $afterLatest) {
            $runDir = $afterLatest
        } else {
            $runDir = $beforeLatest
        }
    }
    if ([string]::IsNullOrWhiteSpace($runDir)) {
        return
    }

    Write-Host "[modelbt] run_dir=$runDir"
    Open-Folder -PathValue $runDir

    $resolved = Resolve-PathFromRoot -PathValue $runDir
    $summaryPath = Join-Path $resolved "summary.json"
    if (Test-Path $summaryPath) {
        Write-Host "[modelbt] summary=$summaryPath"
        Open-Path -PathValue $summaryPath
    }
}

function Invoke-OpenOutputsMenu {
    param([switch]$UseDefaultsMode)
    if ($UseDefaultsMode) {
        Open-Folder -PathValue "data/paper/runs"
        Open-Folder -PathValue "data/backtest/runs"
        Open-Folder -PathValue "docs/reports"
        Open-Folder -PathValue "logs"
        return
    }

    while ($true) {
        Write-Host ""
        Write-Host "== Open Outputs =="
        Write-Host "1) data/paper/runs"
        Write-Host "2) data/backtest/runs"
        Write-Host "3) docs/reports"
        Write-Host "4) logs"
        Write-Host "5) Open all"
        Write-Host "6) Back"
        $choice = Read-Host "Select output"
        switch ($choice) {
            "1" { Open-Folder -PathValue "data/paper/runs" }
            "2" { Open-Folder -PathValue "data/backtest/runs" }
            "3" { Open-Folder -PathValue "docs/reports" }
            "4" { Open-Folder -PathValue "logs" }
            "5" {
                Open-Folder -PathValue "data/paper/runs"
                Open-Folder -PathValue "data/backtest/runs"
                Open-Folder -PathValue "docs/reports"
                Open-Folder -PathValue "logs"
            }
            default { return }
        }
    }
}

function Show-MainMenu {
    Write-Host ""
    Write-Host "Autobot Control Center v1"
    Write-Host "ROOT: $script:ProjectRoot"
    Write-Host "1) Status Dashboard"
    Write-Host "2) Run Daily Pipeline Now"
    Write-Host "3) Start/Stop WS Public Daemon Task"
    Write-Host "4) Backtest Wizard"
    Write-Host "5) Paper Wizard"
    Write-Host "6) Model Train Wizard"
    Write-Host "7) Validate/Doctor"
    Write-Host "8) Open Outputs"
    Write-Host "M) ModelBT Proxy Wizard"
    Write-Host "9) Exit"
}

function Invoke-MenuSelection {
    param(
        [string]$Selection,
        [switch]$UseDefaultsMode,
        [switch]$NoPauseMode
    )
    switch ($selection) {
        "1" {
            if (Invoke-Preflight -Tag "menu1_status") {
                Show-StatusDashboard
            }
            if (-not $NoPauseMode) {
                Wait-ReturnToMenu
            }
        }
        "2" {
            if (Invoke-Preflight -Tag "menu2_daily") {
                Invoke-DailyPipelineNow
            }
            if (-not $NoPauseMode) {
                Wait-ReturnToMenu
            }
        }
        "3" {
            if (Invoke-Preflight -Tag "menu3_task") {
                Invoke-WsDaemonTaskControl
            }
            if (-not $NoPauseMode) {
                Wait-ReturnToMenu
            }
        }
        "4" {
            if (Invoke-Preflight -Tag "menu4_backtest") {
                Invoke-BacktestWizard -UseDefaultsMode:$UseDefaultsMode -DaysOverride $BacktestDaysOverride
            }
            if (-not $NoPauseMode) {
                Wait-ReturnToMenu
            }
        }
        "5" {
            if (Invoke-Preflight -Tag "menu5_paper") {
                Invoke-PaperWizard -UseDefaultsMode:$UseDefaultsMode -DurationSecOverride $PaperDurationSecOverride
            }
            if (-not $NoPauseMode) {
                Wait-ReturnToMenu
            }
        }
        "6" {
            if (Invoke-Preflight -Tag "menu6_model") {
                Invoke-ModelTrainWizard `
                    -UseDefaultsMode:$UseDefaultsMode `
                    -TrainerOverrideValue $TrainerOverride `
                    -LookbackDaysOverride $TrainLookbackDaysOverride
            }
            if (-not $NoPauseMode) {
                Wait-ReturnToMenu
            }
        }
        "7" {
            if (Invoke-Preflight -Tag "menu7_doctor") {
                Invoke-ValidateDoctor -UseDefaultsMode:$UseDefaultsMode
            }
            if (-not $NoPauseMode) {
                Wait-ReturnToMenu
            }
        }
        "8" {
            if (Invoke-Preflight -Tag "menu8_outputs") {
                Invoke-OpenOutputsMenu -UseDefaultsMode:$UseDefaultsMode
            }
            if (-not $NoPauseMode) {
                Wait-ReturnToMenu
            }
        }
        "m" {
            if (Invoke-Preflight -Tag "menu9_modelbt") {
                Invoke-ModelBtWizard -UseDefaultsMode:$UseDefaultsMode -LookbackDaysOverride $ModelBtLookbackDaysOverride
            }
            if (-not $NoPauseMode) {
                Wait-ReturnToMenu
            }
        }
        "9" {
            return $true
        }
        default {
            Write-Host "[warn] invalid menu selection." -ForegroundColor Yellow
            if (-not $NoPauseMode) {
                Wait-ReturnToMenu
            }
        }
    }
    return $false
}

if (-not [string]::IsNullOrWhiteSpace($RunSelection)) {
    [void](Invoke-MenuSelection -Selection $RunSelection -UseDefaultsMode:$UseDefaults -NoPauseMode:$true)
    Write-Host "Autobot Control Center closed."
    exit 0
}

while ($true) {
    Show-MainMenu
    $selection = Read-Host "Select menu (1-9 or M)"
    $exitRequested = Invoke-MenuSelection -Selection $selection -UseDefaultsMode:$UseDefaults -NoPauseMode:$NoPause
    if ($exitRequested) {
        break
    }
}

Write-Host "Autobot Control Center closed."
exit 0
