param(
    [string]$RootDir = "D:\MyApps\Autobot",
    [int]$PaperDurationSec = 7200,
    [int]$BacktestDurationDays = 8,
    [int]$TopN = 20,
    [string]$Quote = "KRW",
    [string]$Tf = "5m"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Write-JsonFile {
    param(
        [Parameter(Mandatory = $true)][string]$Path,
        [Parameter(Mandatory = $true)]$Payload
    )
    $json = $Payload | ConvertTo-Json -Depth 16
    [System.IO.File]::WriteAllText($Path, $json, [System.Text.Encoding]::UTF8)
}

function Get-LatestRunId {
    param(
        [Parameter(Mandatory = $true)][string]$RunsDir,
        [Parameter(Mandatory = $true)][string]$Prefix
    )
    if (-not (Test-Path $RunsDir)) {
        return $null
    }
    $latest = Get-ChildItem -Path $RunsDir -Directory |
        Where-Object { $_.Name -like "$Prefix*" } |
        Sort-Object LastWriteTime -Descending |
        Select-Object -First 1
    if ($null -eq $latest) {
        return $null
    }
    return $latest.Name
}

function Start-And-WaitRun {
    param(
        [Parameter(Mandatory = $true)][string]$Name,
        [Parameter(Mandatory = $true)][string]$RunType,
        [Parameter(Mandatory = $true)][string[]]$CliArgs,
        [Parameter(Mandatory = $true)][string]$WorkDir,
        [Parameter(Mandatory = $true)][string]$LogDir,
        [Parameter(Mandatory = $true)]$CampaignState
    )
    $stdoutPath = Join-Path $LogDir ("{0}_stdout.log" -f $Name)
    $stderrPath = Join-Path $LogDir ("{0}_stderr.log" -f $Name)

    $runsDir = if ($RunType -eq "backtest") {
        Join-Path $WorkDir "data\backtest\runs"
    }
    else {
        Join-Path $WorkDir "data\paper\runs"
    }
    $prefix = if ($RunType -eq "backtest") { "backtest-" } else { "paper-" }

    $before = @{}
    if (Test-Path $runsDir) {
        Get-ChildItem -Path $runsDir -Directory | ForEach-Object {
            $before[$_.Name] = $true
        }
    }

    $CampaignState.current_step = $Name
    $CampaignState.current_status = "running"
    $CampaignState.steps[$Name] = @{
        status = "running"
        started_at = [DateTime]::UtcNow.ToString("o")
        stdout = $stdoutPath
        stderr = $stderrPath
        cli = ("python -m autobot.cli " + ($CliArgs -join " "))
    }
    Write-JsonFile -Path (Join-Path $LogDir "status.json") -Payload $CampaignState

    $fullCliArgs = @("-m", "autobot.cli") + $CliArgs
    $proc = Start-Process -FilePath "python" `
        -ArgumentList $fullCliArgs `
        -WorkingDirectory $WorkDir `
        -RedirectStandardOutput $stdoutPath `
        -RedirectStandardError $stderrPath `
        -PassThru

    $null = $proc.WaitForExit()
    $exitCode = $proc.ExitCode
    if ($null -eq $exitCode) {
        $exitCode = 0
    }

    $after = @()
    if (Test-Path $runsDir) {
        $after = Get-ChildItem -Path $runsDir -Directory |
            Where-Object { $_.Name -like "$prefix*" } |
            Sort-Object LastWriteTime -Descending
    }

    $newRun = $null
    foreach ($item in $after) {
        if (-not $before.ContainsKey($item.Name)) {
            $newRun = $item.Name
            break
        }
    }
    if ($null -eq $newRun) {
        $newRun = Get-LatestRunId -RunsDir $runsDir -Prefix $prefix
    }

    $stepStatus = if ($exitCode -eq 0) { "completed" } else { "failed" }
    $CampaignState.steps[$Name].status = $stepStatus
    $CampaignState.steps[$Name].finished_at = [DateTime]::UtcNow.ToString("o")
    $CampaignState.steps[$Name].exit_code = $exitCode
    $CampaignState.steps[$Name].run_id = $newRun
    $CampaignState.steps[$Name].run_dir = if ($null -ne $newRun) { Join-Path $runsDir $newRun } else { $null }

    if ($exitCode -ne 0) {
        $CampaignState.current_status = "failed"
    }
    else {
        $CampaignState.current_status = "idle"
        $CampaignState.current_step = $null
    }
    Write-JsonFile -Path (Join-Path $LogDir "status.json") -Payload $CampaignState

    if ($exitCode -ne 0) {
        throw "Run failed: $Name (exit=$exitCode)"
    }
}

Push-Location $RootDir
try {
    $campaignDir = Join-Path $RootDir "logs\t15_1_campaign"
    if (-not (Test-Path $campaignDir)) {
        New-Item -Path $campaignDir -ItemType Directory | Out-Null
    }

    $stamp = Get-Date -Format "yyyyMMdd-HHmmss"
    $logDir = Join-Path $campaignDir ("run-{0}" -f $stamp)
    New-Item -Path $logDir -ItemType Directory | Out-Null

    $state = [ordered]@{
        started_at = [DateTime]::UtcNow.ToString("o")
        finished_at = $null
        current_step = $null
        current_status = "starting"
        log_dir = $logDir
        root_dir = $RootDir
        params = @{
            paper_duration_sec = $PaperDurationSec
            backtest_duration_days = $BacktestDurationDays
            top_n = $TopN
            quote = $Quote
            tf = $Tf
        }
        steps = [ordered]@{}
    }
    Write-JsonFile -Path (Join-Path $logDir "status.json") -Payload $state

    Start-And-WaitRun `
        -Name "backtest_off" `
        -RunType "backtest" `
        -CliArgs @(
            "backtest", "run",
            "--tf", $Tf,
            "--quote", $Quote,
            "--top-n", "$TopN",
            "--duration-days", "$BacktestDurationDays",
            "--micro-gate", "off",
            "--micro-order-policy", "off"
        ) `
        -WorkDir $RootDir `
        -LogDir $logDir `
        -CampaignState $state

    Start-And-WaitRun `
        -Name "backtest_on" `
        -RunType "backtest" `
        -CliArgs @(
            "backtest", "run",
            "--tf", $Tf,
            "--quote", $Quote,
            "--top-n", "$TopN",
            "--duration-days", "$BacktestDurationDays",
            "--micro-gate", "off",
            "--micro-order-policy", "on",
            "--micro-order-policy-mode", "trade_only",
            "--micro-order-policy-on-missing", "static_fallback"
        ) `
        -WorkDir $RootDir `
        -LogDir $logDir `
        -CampaignState $state

    Start-And-WaitRun `
        -Name "paper_off" `
        -RunType "paper" `
        -CliArgs @(
            "paper", "run",
            "--duration-sec", "$PaperDurationSec",
            "--quote", $Quote,
            "--top-n", "$TopN",
            "--micro-gate", "off",
            "--micro-order-policy", "off"
        ) `
        -WorkDir $RootDir `
        -LogDir $logDir `
        -CampaignState $state

    Start-And-WaitRun `
        -Name "paper_on" `
        -RunType "paper" `
        -CliArgs @(
            "paper", "run",
            "--duration-sec", "$PaperDurationSec",
            "--quote", $Quote,
            "--top-n", "$TopN",
            "--micro-gate", "off",
            "--micro-order-policy", "on",
            "--micro-order-policy-mode", "trade_only",
            "--micro-order-policy-on-missing", "static_fallback"
        ) `
        -WorkDir $RootDir `
        -LogDir $logDir `
        -CampaignState $state

    $state.current_status = "completed"
    $state.current_step = $null
    $state.finished_at = [DateTime]::UtcNow.ToString("o")
    Write-JsonFile -Path (Join-Path $logDir "status.json") -Payload $state

    $result = @{
        log_dir = $logDir
        status_path = Join-Path $logDir "status.json"
        run_ids = @{
            backtest_off = $state.steps.backtest_off.run_id
            backtest_on = $state.steps.backtest_on.run_id
            paper_off = $state.steps.paper_off.run_id
            paper_on = $state.steps.paper_on.run_id
        }
    }
    Write-JsonFile -Path (Join-Path $logDir "result.json") -Payload $result
}
catch {
    if ($null -ne $state) {
        $state.current_status = "failed"
        $state.finished_at = [DateTime]::UtcNow.ToString("o")
        $state.error = $_.Exception.Message
        Write-JsonFile -Path (Join-Path $logDir "status.json") -Payload $state
    }
    throw
}
finally {
    Pop-Location
}
