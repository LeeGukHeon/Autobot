param(
    [string]$ProjectRoot = "",
    [string]$PythonExe = "",
    [string]$SummaryPath = "data/raw_trade_v1/_meta/raw_trade_v1_latest.json",
    [string]$Quote = "KRW",
    [int]$WindowDays = 2,
    [string]$RawWsRoot = "data/raw_ws/upbit/public",
    [string]$RawTicksRoot = "data/raw_ticks/upbit/trades",
    [string]$OutRoot = "data/raw_trade_v1",
    [string]$MetaDir = "data/raw_trade_v1/_meta",
    [string]$PreferSourceOrder = "ws,rest",
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
    Write-Host ("[raw-trade-v1-refresh] step={0}" -f $StepName)
    Write-Host ("[raw-trade-v1-refresh] command={0}" -f $commandText)
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
$resolvedSummaryPath = Resolve-ProjectPath -Root $resolvedProjectRoot -PathValue $SummaryPath
$resolvedRawWsRoot = Resolve-ProjectPath -Root $resolvedProjectRoot -PathValue $RawWsRoot
$resolvedRawTicksRoot = Resolve-ProjectPath -Root $resolvedProjectRoot -PathValue $RawTicksRoot
$resolvedOutRoot = Resolve-ProjectPath -Root $resolvedProjectRoot -PathValue $OutRoot
$resolvedMetaDir = Resolve-ProjectPath -Root $resolvedProjectRoot -PathValue $MetaDir
$windowDaysEffective = [Math]::Max([int]$WindowDays, 1)
$endDateUtc = (Get-Date).ToUniversalTime().Date
$startDateUtc = $endDateUtc.AddDays(-1 * ($windowDaysEffective - 1))
$startDateText = $startDateUtc.ToString("yyyy-MM-dd")
$endDateText = $endDateUtc.ToString("yyyy-MM-dd")

$buildArgs = @(
    "-m", "autobot.cli",
    "collect", "raw-trade-v1",
    "--start", $startDateText,
    "--end", $endDateText,
    "--quote", $Quote,
    "--raw-ws-root", $resolvedRawWsRoot,
    "--raw-ticks-root", $resolvedRawTicksRoot,
    "--out-root", $resolvedOutRoot,
    "--meta-dir", $resolvedMetaDir,
    "--prefer-source-order", $PreferSourceOrder
)

$stepResults = @()
Push-Location $resolvedProjectRoot
try {
    $stepResults += ,(Invoke-ProjectPythonStep -PythonPath $resolvedPythonExe -StepName "build_raw_trade_v1" -ArgList $buildArgs)
} finally {
    Pop-Location
}

$summary = [ordered]@{
    policy = "raw_trade_v1_refresh_v1"
    generated_at_utc = (Get-Date).ToUniversalTime().ToString("o")
    project_root = $resolvedProjectRoot
    python_exe = $resolvedPythonExe
    quote = $Quote
    window_days = $windowDaysEffective
    start_date_utc = $startDateText
    end_date_utc = $endDateText
    raw_ws_root = $resolvedRawWsRoot
    raw_ticks_root = $resolvedRawTicksRoot
    out_root = $resolvedOutRoot
    meta_dir = $resolvedMetaDir
    steps = @($stepResults)
}
$summaryDir = Split-Path -Parent $resolvedSummaryPath
if (-not [string]::IsNullOrWhiteSpace($summaryDir)) {
    New-Item -ItemType Directory -Force -Path $summaryDir | Out-Null
}
$summary | ConvertTo-Json -Depth 8 | Set-Content -Path $resolvedSummaryPath -Encoding UTF8
Write-Host $resolvedSummaryPath
