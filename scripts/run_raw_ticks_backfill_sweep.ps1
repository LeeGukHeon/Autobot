param(
    [string]$ProjectRoot = "",
    [string]$PythonExe = "",
    [string]$SummaryPath = "data/raw_ticks/upbit/_meta/ticks_backfill_latest.json",
    [string]$PlanPath = "data/raw_ticks/upbit/_meta/ticks_plan_backfill_auto.json",
    [string]$Quote = "KRW",
    [int]$TopN = 30,
    [string]$DaysAgoCsv = "1,2",
    [string]$RawRoot = "data/raw_ticks/upbit/trades",
    [string]$MetaDir = "data/raw_ticks/upbit/_meta",
    [int]$Workers = 1,
    [int]$MaxPagesPerTarget = 50,
    [string]$RateLimitStrict = "true",
    [switch]$DryRun
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

$baseScript = Join-Path $PSScriptRoot "run_raw_ticks_daily.ps1"
$invokeParams = @{
    ProjectRoot = $ProjectRoot
    PythonExe = $PythonExe
    SummaryPath = $SummaryPath
    PlanPath = $PlanPath
    Quote = $Quote
    TopN = $TopN
    DaysAgoCsv = $DaysAgoCsv
    RawRoot = $RawRoot
    MetaDir = $MetaDir
    Workers = $Workers
    MaxPagesPerTarget = $MaxPagesPerTarget
    RateLimitStrict = $RateLimitStrict
}
if ($DryRun) {
    $invokeParams["DryRun"] = $true
}
& $baseScript @invokeParams
if (($null -ne (Get-Variable LASTEXITCODE -Scope Global -ErrorAction SilentlyContinue)) -and ($LASTEXITCODE -ne 0)) {
    exit $LASTEXITCODE
}
