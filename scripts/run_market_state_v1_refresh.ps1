param(
    [string]$ProjectRoot = "",
    [string]$PythonExe = "",
    [int]$DaysAgo = 1,
    [string]$SummaryPath = "data/derived/market_state_v1/_meta/market_state_v1_refresh_latest.json",
    [string]$Quote = "KRW",
    [string]$RawWsRoot = "data/raw_ws/upbit/public",
    [string]$RawTradeRoot = "data/raw_trade_v1",
    [string]$CandlesRoot = "data/parquet/candles_api_v1",
    [string]$MarketStateRoot = "data/derived/market_state_v1",
    [string]$TradeableLabelRoot = "data/derived/tradeable_label_v1",
    [string]$NetEdgeLabelRoot = "data/derived/net_edge_label_v1",
    [string]$TrainingSliceRoot = "data/derived/market_state_training_slice_v1",
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

function Invoke-ProjectPythonStep {
    param(
        [string]$PythonPath,
        [string]$StepName,
        [string[]]$ArgList
    )
    $commandText = $PythonPath + " " + (($ArgList | ForEach-Object { Quote-ShellArg ([string]$_) }) -join " ")
    Write-Host ("[market-state-v1-refresh] step={0}" -f $StepName)
    Write-Host ("[market-state-v1-refresh] command={0}" -f $commandText)
    if ($DryRun) {
        return [ordered]@{
            step = $StepName
            command = $commandText
            exit_code = 0
            dry_run = $true
            output_preview = ""
        }
    }
    $output = & $PythonPath @ArgList 2>&1
    $exitCode = [int]$LASTEXITCODE
    $outputText = [string]($output -join [Environment]::NewLine)
    if (-not [string]::IsNullOrWhiteSpace($outputText)) {
        Write-Host $outputText
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

function Resolve-KstTimezone {
    foreach ($candidate in @("Asia/Seoul", "Korea Standard Time")) {
        try {
            return [System.TimeZoneInfo]::FindSystemTimeZoneById($candidate)
        } catch {
        }
    }
    throw "Asia/Seoul timezone id not found"
}

function Resolve-OperatingDateKst {
    param([int]$DaysAgoValue)
    $kst = Resolve-KstTimezone
    $utcNow = (Get-Date).ToUniversalTime()
    $kstNow = [System.TimeZoneInfo]::ConvertTimeFromUtc($utcNow, $kst)
    return $kstNow.Date.AddDays(-1 * [Math]::Max([int]$DaysAgoValue, 1)).ToString("yyyy-MM-dd")
}

$resolvedProjectRoot = if ([string]::IsNullOrWhiteSpace($ProjectRoot)) { Resolve-DefaultProjectRoot } else { $ProjectRoot }
$resolvedProjectRoot = [System.IO.Path]::GetFullPath($resolvedProjectRoot)
$resolvedPythonExe = if ([string]::IsNullOrWhiteSpace($PythonExe)) { Resolve-DefaultPythonExe -Root $resolvedProjectRoot } else { $PythonExe }
$resolvedSummaryPath = Resolve-ProjectPath -Root $resolvedProjectRoot -PathValue $SummaryPath
$resolvedRawWsRoot = Resolve-ProjectPath -Root $resolvedProjectRoot -PathValue $RawWsRoot
$resolvedRawTradeRoot = Resolve-ProjectPath -Root $resolvedProjectRoot -PathValue $RawTradeRoot
$resolvedCandlesRoot = Resolve-ProjectPath -Root $resolvedProjectRoot -PathValue $CandlesRoot
$resolvedMarketStateRoot = Resolve-ProjectPath -Root $resolvedProjectRoot -PathValue $MarketStateRoot
$resolvedTradeableLabelRoot = Resolve-ProjectPath -Root $resolvedProjectRoot -PathValue $TradeableLabelRoot
$resolvedNetEdgeLabelRoot = Resolve-ProjectPath -Root $resolvedProjectRoot -PathValue $NetEdgeLabelRoot
$resolvedTrainingSliceRoot = Resolve-ProjectPath -Root $resolvedProjectRoot -PathValue $TrainingSliceRoot
$operatingDateKst = Resolve-OperatingDateKst -DaysAgoValue $DaysAgo

$buildArgs = @(
    "-m", "autobot.cli",
    "data", "build-market-state-v1",
    "--start", $operatingDateKst,
    "--end", $operatingDateKst,
    "--quote", $Quote,
    "--raw-ws-root", $resolvedRawWsRoot,
    "--raw-trade-root", $resolvedRawTradeRoot,
    "--candles-root", $resolvedCandlesRoot,
    "--market-state-root", $resolvedMarketStateRoot,
    "--tradeable-label-root", $resolvedTradeableLabelRoot,
    "--net-edge-label-root", $resolvedNetEdgeLabelRoot,
    "--skip-existing-complete", "true"
)

$sliceArgs = @(
    "-m", "autobot.cli",
    "data", "build-market-state-training-slice-v1",
    "--start", $operatingDateKst,
    "--end", $operatingDateKst,
    "--quote", $Quote,
    "--market-state-root", $resolvedMarketStateRoot,
    "--tradeable-label-root", $resolvedTradeableLabelRoot,
    "--net-edge-label-root", $resolvedNetEdgeLabelRoot,
    "--out-root", $resolvedTrainingSliceRoot,
    "--skip-existing-complete", "true"
)

$stepResults = @()
Push-Location $resolvedProjectRoot
try {
    $stepResults += ,(Invoke-ProjectPythonStep -PythonPath $resolvedPythonExe -StepName "build_market_state_v1" -ArgList $buildArgs)
    $stepResults += ,(Invoke-ProjectPythonStep -PythonPath $resolvedPythonExe -StepName "build_market_state_training_slice_v1" -ArgList $sliceArgs)
} finally {
    Pop-Location
}

$marketStateLatestPath = Join-Path $resolvedMarketStateRoot "_meta/market_state_v1_latest.json"
$trainingSliceLatestPath = Join-Path $resolvedTrainingSliceRoot "_meta/market_state_training_slice_v1_latest.json"
$marketStateLatest = @{}
$trainingSliceLatest = @{}
if ((-not $DryRun) -and (Test-Path $marketStateLatestPath)) {
    try {
        $marketStateLatest = Get-Content -Raw -Path $marketStateLatestPath -Encoding UTF8 | ConvertFrom-Json -AsHashtable
    } catch {
        $marketStateLatest = @{}
    }
}
if ((-not $DryRun) -and (Test-Path $trainingSliceLatestPath)) {
    try {
        $trainingSliceLatest = Get-Content -Raw -Path $trainingSliceLatestPath -Encoding UTF8 | ConvertFrom-Json -AsHashtable
    } catch {
        $trainingSliceLatest = @{}
    }
}

$summary = [ordered]@{
    policy = "market_state_v1_refresh_v1"
    generated_at_utc = (Get-Date).ToUniversalTime().ToString("o")
    project_root = $resolvedProjectRoot
    python_exe = $resolvedPythonExe
    quote = $Quote
    operating_date_kst = $operatingDateKst
    raw_ws_root = $resolvedRawWsRoot
    raw_trade_root = $resolvedRawTradeRoot
    candles_root = $resolvedCandlesRoot
    market_state_root = $resolvedMarketStateRoot
    tradeable_label_root = $resolvedTradeableLabelRoot
    net_edge_label_root = $resolvedNetEdgeLabelRoot
    training_slice_root = $resolvedTrainingSliceRoot
    steps = @($stepResults)
    market_state_latest = $marketStateLatest
    training_slice_latest = $trainingSliceLatest
}
$summaryDir = Split-Path -Parent $resolvedSummaryPath
if (-not [string]::IsNullOrWhiteSpace($summaryDir)) {
    New-Item -ItemType Directory -Force -Path $summaryDir | Out-Null
}
$summary | ConvertTo-Json -Depth 10 | Set-Content -Path $resolvedSummaryPath -Encoding UTF8
Write-Host $resolvedSummaryPath
