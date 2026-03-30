param(
    [string]$ProjectRoot = "",
    [string]$PythonExe = "",
    [string]$MetaDir = "data/collect/_meta",
    [string]$SummaryPath = "data/collect/_meta/candles_api_refresh_latest.json",
    [string]$Quote = "KRW",
    [string]$MarketMode = "top_n_by_recent_value_est",
    [int]$TopN = 50,
    [string]$BaseDataset = "candles_api_v1",
    [string]$OutDataset = "candles_api_v1",
    [string]$PlanPath = "data/collect/_meta/candle_topup_plan_auto.json",
    [int]$LookbackMonths = 3,
    [string]$Tf = "1m,5m,15m,60m,240m",
    [int]$MaxBackfillDays1m = 3,
    [string[]]$Markets = @(),
    [int]$Workers = 1,
    [int]$MaxRequests = 120,
    [string]$RateLimitStrict = "true",
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
    Write-Host ("[candles-api-refresh] step={0}" -f $StepName)
    Write-Host ("[candles-api-refresh] command={0}" -f $commandText)
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

$resolvedProjectRoot = if ([string]::IsNullOrWhiteSpace($ProjectRoot)) { Resolve-DefaultProjectRoot } else { $ProjectRoot }
$resolvedProjectRoot = [System.IO.Path]::GetFullPath($resolvedProjectRoot)
$resolvedPythonExe = if ([string]::IsNullOrWhiteSpace($PythonExe)) { Resolve-DefaultPythonExe -Root $resolvedProjectRoot } else { $PythonExe }
$resolvedMetaDir = Resolve-ProjectPath -Root $resolvedProjectRoot -PathValue $MetaDir
$resolvedSummaryPath = Resolve-ProjectPath -Root $resolvedProjectRoot -PathValue $SummaryPath
$resolvedPlanPath = Resolve-ProjectPath -Root $resolvedProjectRoot -PathValue $PlanPath
$serializedMarkets = Join-DelimitedStringArray -Values $Markets
$validateReportPath = Join-Path $resolvedMetaDir "candle_validate_report.json"

$planArgs = @(
    "-m", "autobot.cli",
    "collect", "plan-candles",
    "--base-dataset", $BaseDataset,
    "--parquet-root", "data/parquet",
    "--out", $resolvedPlanPath,
    "--lookback-months", ([string]([Math]::Max([int]$LookbackMonths, 1))),
    "--tf", $Tf,
    "--quote", $Quote,
    "--market-mode", $MarketMode,
    "--top-n", ([string]([Math]::Max([int]$TopN, 1))),
    "--max-backfill-days-1m", ([string]([Math]::Max([int]$MaxBackfillDays1m, 1)))
)
if (-not [string]::IsNullOrWhiteSpace($serializedMarkets)) {
    $planArgs += @("--markets", $serializedMarkets)
}

$collectArgs = @(
    "-m", "autobot.cli",
    "collect", "candles",
    "--plan", $resolvedPlanPath,
    "--out-dataset", $OutDataset,
    "--parquet-root", "data/parquet",
    "--collect-meta-dir", $resolvedMetaDir,
    "--validate-report", $validateReportPath,
    "--workers", ([string]([Math]::Max([int]$Workers, 1))),
    "--dry-run", "false",
    "--max-requests", ([string]([Math]::Max([int]$MaxRequests, 1))),
    "--rate-limit-strict", $RateLimitStrict
)

$stepResults = @()
Push-Location $resolvedProjectRoot
try {
    $stepResults += ,(Invoke-ProjectPythonStep -PythonPath $resolvedPythonExe -StepName "plan_candles_api" -ArgList $planArgs)
    $stepResults += ,(Invoke-ProjectPythonStep -PythonPath $resolvedPythonExe -StepName "collect_candles_api" -ArgList $collectArgs)
} finally {
    Pop-Location
}

$summary = [ordered]@{
    policy = "candles_api_refresh_v1"
    generated_at_utc = (Get-Date).ToUniversalTime().ToString("o")
    project_root = $resolvedProjectRoot
    python_exe = $resolvedPythonExe
    meta_dir = $resolvedMetaDir
    plan_path = $resolvedPlanPath
    validate_report_path = $validateReportPath
    steps = @($stepResults)
}
$summaryDir = Split-Path -Parent $resolvedSummaryPath
if (-not [string]::IsNullOrWhiteSpace($summaryDir)) {
    New-Item -ItemType Directory -Force -Path $summaryDir | Out-Null
}
$summary | ConvertTo-Json -Depth 8 | Set-Content -Path $resolvedSummaryPath -Encoding UTF8
Write-Host $resolvedSummaryPath
