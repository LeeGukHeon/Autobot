param(
    [string]$ProjectRoot = "",
    [string]$PythonExe = "",
    [string]$MetaDir = "data/collect/_meta",
    [string]$SummaryPath = "data/collect/_meta/data_platform_refresh_latest.json",
    [string]$Quote = "KRW",
    [string]$MarketMode = "top_n_by_recent_value_est",
    [int]$TopN = 50,
    [string]$SecondBaseDataset = "candles_second_v1",
    [string]$SecondMarketSourceDataset = "candles_api_v1",
    [string]$SecondPlanPath = "data/collect/_meta/candle_second_plan.json",
    [int]$SecondMaxBackfillDays = 7,
    [int]$CandlesMaxRequests = 120,
    [string]$WsCandleBaseDataset = "ws_candle_v1",
    [string]$WsCandleMarketSourceDataset = "candles_api_v1",
    [string]$WsCandlePlanPath = "data/collect/_meta/ws_candle_plan.json",
    [string]$WsCandleTf = "1s,1m",
    [int]$WsCandleDurationSec = 1800,
    [string]$Lob30BaseDataset = "lob30_v1",
    [string]$Lob30MarketSourceDataset = "candles_api_v1",
    [string]$Lob30PlanPath = "data/collect/_meta/lob30_plan.json",
    [int]$Lob30DurationSec = 180,
    [string[]]$TensorMarkets = @(),
    [int]$TensorMaxMarkets = 20,
    [int]$TensorMaxAnchorsPerMarket = 64,
    [int]$TensorRecentDates = 2,
    [string]$MicroOutRoot = "data/parquet/micro_v1",
    [string]$MicroRawTicksRoot = "data/raw_ticks/upbit/trades",
    [string]$MicroRawWsRoot = "data/raw_ws/upbit/public",
    [string]$MicroBaseCandles = "candles_api_v1",
    [int]$MicroRecentDates = 2,
    [int]$TensorSecondLookbackSteps = 120,
    [int]$TensorMinuteLookbackSteps = 30,
    [int]$TensorMicroLookbackSteps = 30,
    [int]$TensorLobLookbackSteps = 32,
    [string]$PublishLockFile = "/tmp/autobot-train-acceptance.lock",
    [switch]$DryRun
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

. (Join-Path $PSScriptRoot "systemd_service_utils.ps1")

function Resolve-ProjectPath {
    param(
        [string]$Root,
        [string]$PathValue
    )
    if ([string]::IsNullOrWhiteSpace($PathValue)) {
        return $Root
    }
    if ([System.IO.Path]::IsPathRooted($PathValue)) {
        return [System.IO.Path]::GetFullPath($PathValue)
    }
    return [System.IO.Path]::GetFullPath((Join-Path $Root $PathValue))
}

function Format-CommandLine {
    param(
        [string]$Exe,
        [string[]]$ArgList
    )
    return ($Exe + " " + (($ArgList | ForEach-Object { Quote-ShellArg ([string]$_) }) -join " "))
}

function Invoke-ProjectPythonStep {
    param(
        [string]$PythonPath,
        [string]$StepName,
        [string[]]$ArgList,
        [string]$LockFile = "",
        [switch]$BlockingLock
    )
    $commandText = Format-CommandLine -Exe $PythonPath -ArgList $ArgList
    Write-Host ("[data-platform-refresh] step={0}" -f $StepName)
    Write-Host ("[data-platform-refresh] command={0}" -f $commandText)
    if (-not [string]::IsNullOrWhiteSpace($LockFile)) {
        Write-Host ("[data-platform-refresh] lock_file={0}" -f $LockFile)
    }
    if ($DryRun) {
        return [ordered]@{
            step = $StepName
            command = $commandText
            exit_code = 0
            dry_run = $true
            output_preview = ""
        }
    }
    if ([string]::IsNullOrWhiteSpace($LockFile)) {
        $output = & $PythonPath @ArgList 2>&1
        $exitCode = [int]$LASTEXITCODE
    } else {
        $bashExe = "/bin/bash"
        $quotedCommand = Quote-ShellArg $commandText
        $quotedLockFile = Quote-ShellArg $LockFile
        if ($BlockingLock) {
            $lockCommand = "if command -v flock >/dev/null 2>&1; then flock " + $quotedLockFile + " bash -lc " + $quotedCommand + "; else bash -lc " + $quotedCommand + "; fi"
        } else {
            $lockCommand = "if command -v flock >/dev/null 2>&1; then flock -n " + $quotedLockFile + " bash -lc " + $quotedCommand + "; status=$?; if [ $status -eq 1 ]; then echo lock busy, skipping; exit 0; else exit $status; fi; else bash -lc " + $quotedCommand + "; fi"
        }
        $output = & $bashExe -lc $lockCommand 2>&1
        $exitCode = [int]$LASTEXITCODE
    }
    $outputText = [string]($output -join [Environment]::NewLine)
    if (-not [string]::IsNullOrWhiteSpace($outputText)) {
        Write-Host $outputText
    }
    if (($exitCode -ne 0) -and ([string]$StepName -eq "collect_sequence_tensors")) {
        $buildReportPath = Join-Path (Join-Path (Join-Path $resolvedProjectRoot "data") "parquet") "sequence_v1/_meta/build_report.json"
        if (Test-Path $buildReportPath) {
            try {
                $buildReport = Get-Content -Path $buildReportPath -Raw -Encoding UTF8 | ConvertFrom-Json
                $builtAnchors = [int]($buildReport.built_anchors)
                if ($builtAnchors -gt 0) {
                    Write-Warning ("[data-platform-refresh] tolerating partial sequence tensor build because built_anchors={0}" -f $builtAnchors)
                    $exitCode = 0
                }
            } catch {
            }
        }
    }
    if ($exitCode -ne 0) {
        throw ("step failed: " + $StepName + " exit_code=" + $exitCode)
    }
    return [ordered]@{
        step = $StepName
        command = $commandText
        exit_code = $exitCode
        dry_run = $false
        output_preview = if ([string]::IsNullOrWhiteSpace($outputText)) { "" } elseif ($outputText.Length -le 2000) { $outputText } else { $outputText.Substring(0, 2000) }
    }
}

function Get-RecentUtcDateValues {
    param([int]$Count)
    $resolvedCount = [Math]::Max([int]$Count, 1)
    $todayUtc = (Get-Date).ToUniversalTime().Date
    $values = @()
    for ($offset = 0; $offset -lt $resolvedCount; $offset++) {
        $values += $todayUtc.AddDays(-$offset).ToString("yyyy-MM-dd")
    }
    return @($values)
}

$resolvedProjectRoot = if ([string]::IsNullOrWhiteSpace($ProjectRoot)) { Resolve-DefaultProjectRoot } else { $ProjectRoot }
$resolvedProjectRoot = [System.IO.Path]::GetFullPath($resolvedProjectRoot)
$resolvedPythonExe = if ([string]::IsNullOrWhiteSpace($PythonExe)) { Resolve-DefaultPythonExe -Root $resolvedProjectRoot } else { $PythonExe }
$resolvedMetaDir = Resolve-ProjectPath -Root $resolvedProjectRoot -PathValue $MetaDir
$resolvedSummaryPath = Resolve-ProjectPath -Root $resolvedProjectRoot -PathValue $SummaryPath
$resolvedSecondPlanPath = Resolve-ProjectPath -Root $resolvedProjectRoot -PathValue $SecondPlanPath
$resolvedWsCandlePlanPath = Resolve-ProjectPath -Root $resolvedProjectRoot -PathValue $WsCandlePlanPath
$resolvedLob30PlanPath = Resolve-ProjectPath -Root $resolvedProjectRoot -PathValue $Lob30PlanPath
$resolvedMicroOutRoot = Resolve-ProjectPath -Root $resolvedProjectRoot -PathValue $MicroOutRoot
$resolvedMicroRawTicksRoot = Resolve-ProjectPath -Root $resolvedProjectRoot -PathValue $MicroRawTicksRoot
$resolvedMicroRawWsRoot = Resolve-ProjectPath -Root $resolvedProjectRoot -PathValue $MicroRawWsRoot
$serializedTensorMarkets = Join-DelimitedStringArray -Values $TensorMarkets

$steps = @(
    [ordered]@{
        name = "plan_candles_second"
        args = @(
            "-m", "autobot.cli",
            "collect", "plan-candles",
            "--base-dataset", $SecondBaseDataset,
            "--market-source-dataset", $SecondMarketSourceDataset,
            "--out", $resolvedSecondPlanPath,
            "--tf", "1s",
            "--quote", $Quote,
            "--market-mode", $MarketMode,
            "--top-n", ([string]([Math]::Max([int]$TopN, 1))),
            "--max-backfill-days-1s", ([string]([Math]::Max([int]$SecondMaxBackfillDays, 1))),
            "--max-backfill-days-1m", ([string]([Math]::Max([int]$SecondMaxBackfillDays, 1)))
        )
    }
    [ordered]@{
        name = "collect_candles_second"
        args = @(
            "-m", "autobot.cli",
            "collect", "candles",
            "--plan", $resolvedSecondPlanPath,
            "--out-dataset", "candles_second_v1",
            "--collect-meta-dir", $resolvedMetaDir,
            "--validate-report", (Join-Path $resolvedMetaDir "candle_second_validate_report.json"),
            "--workers", "1",
            "--dry-run", "false",
            "--max-requests", ([string]([Math]::Max([int]$CandlesMaxRequests, 1))),
            "--stop-on-first-fail", "true",
            "--rate-limit-strict", "true"
        )
    }
    [ordered]@{
        name = "plan_ws_candles"
        args = @(
            "-m", "autobot.cli",
            "collect", "plan-ws-candles",
            "--base-dataset", $WsCandleBaseDataset,
            "--market-source-dataset", $WsCandleMarketSourceDataset,
            "--out", $resolvedWsCandlePlanPath,
            "--quote", $Quote,
            "--market-mode", $MarketMode,
            "--top-n", ([string]([Math]::Max([int]$TopN, 1))),
            "--tf", $WsCandleTf
        )
    }
    [ordered]@{
        name = "collect_ws_candles"
        args = @(
            "-m", "autobot.cli",
            "collect", "ws-candles",
            "--plan", $resolvedWsCandlePlanPath,
            "--out-dataset", "ws_candle_v1",
            "--meta-dir", $resolvedMetaDir,
            "--duration-sec", ([string]([Math]::Max([int]$WsCandleDurationSec, 1))),
            "--rate-limit-strict", "true"
        )
    }
    [ordered]@{
        name = "plan_lob30"
        args = @(
            "-m", "autobot.cli",
            "collect", "plan-lob30",
            "--base-dataset", $Lob30BaseDataset,
            "--market-source-dataset", $Lob30MarketSourceDataset,
            "--out", $resolvedLob30PlanPath,
            "--quote", $Quote,
            "--market-mode", $MarketMode,
            "--top-n", ([string]([Math]::Max([int]$TopN, 1)))
        )
    }
    [ordered]@{
        name = "collect_lob30"
        args = @(
            "-m", "autobot.cli",
            "collect", "lob30",
            "--plan", $resolvedLob30PlanPath,
            "--out-dataset", "lob30_v1",
            "--meta-dir", $resolvedMetaDir,
            "--duration-sec", ([string]([Math]::Max([int]$Lob30DurationSec, 1))),
            "--rate-limit-strict", "true"
        )
    }
    [ordered]@{
        name = "refresh_data_contract_registry"
        args = @(
            "-m", "autobot.ops.data_contract_registry",
            "--project-root", $resolvedProjectRoot
        )
        lock_file = $PublishLockFile
        blocking_lock = $true
    }
    [ordered]@{
        name = "publish_data_platform_snapshot"
        args = @(
            "-m", "autobot.ops.data_platform_snapshot",
            "publish",
            "--project-root", $resolvedProjectRoot
        )
        lock_file = $PublishLockFile
        blocking_lock = $true
    }
)

$tensorDateValues = Get-RecentUtcDateValues -Count $TensorRecentDates
$microDateValues = Get-RecentUtcDateValues -Count $MicroRecentDates
$microDateStart = [string]$microDateValues[-1]
$microDateEnd = [string]$microDateValues[0]
$microSteps = @(
    [ordered]@{
        name = "aggregate_micro_current_window"
        args = @(
            "-m", "autobot.cli",
            "micro", "aggregate",
            "--start", $microDateStart,
            "--end", $microDateEnd,
            "--tf", "1m,5m",
            "--quote", $Quote,
            "--top-n", ([string]([Math]::Max([int]$TopN, 1))),
            "--raw-ticks-root", $resolvedMicroRawTicksRoot,
            "--raw-ws-root", $resolvedMicroRawWsRoot,
            "--out-root", $resolvedMicroOutRoot,
            "--base-candles", $MicroBaseCandles,
            "--mode", "overwrite"
        )
    }
    [ordered]@{
        name = "validate_micro_current_window"
        args = @(
            "-m", "autobot.cli",
            "micro", "validate",
            "--out-root", $resolvedMicroOutRoot,
            "--tf", "1m,5m",
            "--base-candles", $MicroBaseCandles
        )
    }
)
$tensorSteps = @()
for ($index = 0; $index -lt $tensorDateValues.Count; $index++) {
    $dateValue = [string]$tensorDateValues[$index]
    $stepName = if ($index -eq 0) { "collect_sequence_tensors" } else { "collect_sequence_tensors_prev$index" }
    $tensorArgs = @(
        "-m", "autobot.cli",
        "collect", "tensors",
        "--out-dataset", "sequence_v1",
        "--date", $dateValue,
        "--max-markets", ([string]([Math]::Max([int]$TensorMaxMarkets, 1))),
        "--max-anchors-per-market", ([string]([Math]::Max([int]$TensorMaxAnchorsPerMarket, 1))),
        "--second-lookback-steps", ([string]([Math]::Max([int]$TensorSecondLookbackSteps, 1))),
        "--minute-lookback-steps", ([string]([Math]::Max([int]$TensorMinuteLookbackSteps, 1))),
        "--micro-lookback-steps", ([string]([Math]::Max([int]$TensorMicroLookbackSteps, 1))),
        "--lob-lookback-steps", ([string]([Math]::Max([int]$TensorLobLookbackSteps, 1))),
        "--skip-existing-ready", "false"
    )
    $tensorSteps += ,([ordered]@{
        name = $stepName
        args = $tensorArgs
    })
}
$insertIndex = [Math]::Max($steps.Count - 1, 0)
$prefixSteps = if ($insertIndex -gt 0) { @($steps[0..($insertIndex - 1)]) } else { @() }
$suffixSteps = if ($insertIndex -lt $steps.Count) { @($steps[$insertIndex..($steps.Count - 1)]) } else { @() }
$steps = @($prefixSteps + $microSteps + $tensorSteps + $suffixSteps)

if (-not [string]::IsNullOrWhiteSpace($serializedTensorMarkets)) {
    foreach ($step in @($steps)) {
        if ([string]$step.name -ne "collect_sequence_tensors") {
            continue
        }
        $step.args += @("--markets", $serializedTensorMarkets)
    }
}

if ($DryRun) {
    Write-Host ("[data-platform-refresh][dry-run] project_root={0}" -f $resolvedProjectRoot)
    Write-Host ("[data-platform-refresh][dry-run] meta_dir={0}" -f $resolvedMetaDir)
    Write-Host ("[data-platform-refresh][dry-run] summary_path={0}" -f $resolvedSummaryPath)
    Write-Host ("[data-platform-refresh][dry-run] publish_lock_file={0}" -f $PublishLockFile)
}

$stepResults = @()
Push-Location $resolvedProjectRoot
try {
    foreach ($step in @($steps)) {
        $stepLockFile = ""
        if ($step.Contains('lock_file')) {
            $stepLockFile = [string]$step.lock_file
        }
        $stepBlockingLock = $false
        if ($step.Contains('blocking_lock')) {
            $stepBlockingLock = [bool]$step.blocking_lock
        }
        $stepResults += ,(Invoke-ProjectPythonStep `
            -PythonPath $resolvedPythonExe `
            -StepName ([string]$step.name) `
            -ArgList @($step.args) `
            -LockFile $stepLockFile `
            -BlockingLock:$stepBlockingLock)
    }
} finally {
    Pop-Location
}

$summary = [ordered]@{
    policy = "data_platform_refresh_v1"
    generated_at_utc = (Get-Date).ToUniversalTime().ToString("o")
    project_root = $resolvedProjectRoot
    python_exe = $resolvedPythonExe
    meta_dir = $resolvedMetaDir
    steps = @($stepResults)
}
$summaryDir = Split-Path -Parent $resolvedSummaryPath
if (-not [string]::IsNullOrWhiteSpace($summaryDir)) {
    New-Item -ItemType Directory -Force -Path $summaryDir | Out-Null
}
$summary | ConvertTo-Json -Depth 8 | Set-Content -Path $resolvedSummaryPath -Encoding UTF8
Write-Host $resolvedSummaryPath
